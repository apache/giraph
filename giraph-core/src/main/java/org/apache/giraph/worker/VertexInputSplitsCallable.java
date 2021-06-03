/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.worker;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.mapping.translate.TranslateEdge;
import org.apache.giraph.io.InputType;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

/**
 * Load as many vertex input splits as possible.
 * Every thread will has its own instance of WorkerClientRequestProcessor
 * to send requests.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("unchecked")
public class VertexInputSplitsCallable<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends InputSplitsCallable<I, V, E> {
  /** How often to update metrics and print info */
  public static final int VERTICES_UPDATE_PERIOD = 250000;
  /** How often to update filtered out metrics */
  public static final int VERTICES_FILTERED_UPDATE_PERIOD = 2500;

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(VertexInputSplitsCallable.class);
  /** Vertex input format */
  private final VertexInputFormat<I, V, E> vertexInputFormat;
  /** Input split max vertices (-1 denotes all) */
  private final long inputSplitMaxVertices;
  /** Bsp service worker (only use thread-safe methods) */
  private final BspServiceWorker<I, V, E> bspServiceWorker;
  /** Filter to select which vertices to keep */
  private final VertexInputFilter<I, V, E> vertexInputFilter;
  /** Can embedInfo in vertexIds */
  private final boolean canEmbedInIds;
  /**
   * Whether the chosen {@link OutEdges} implementation allows for Edge
   * reuse.
   */
  private final boolean reuseEdgeObjects;
  /** Used to translate Edges during vertex input phase based on localData */
  private final TranslateEdge<I, E> translateEdge;

  // Metrics
  /** number of vertices loaded meter across all readers */
  private final Meter totalVerticesMeter;
  /** number of vertices filtered out */
  private final Counter totalVerticesFilteredCounter;
  /** number of edges loaded meter across all readers */
  private final Meter totalEdgesMeter;

  /**
   * Constructor.
   *
   * @param vertexInputFormat Vertex input format
   * @param context Context
   * @param configuration Configuration
   * @param bspServiceWorker service worker
   * @param splitsHandler Handler for input splits
   */
  public VertexInputSplitsCallable(
      VertexInputFormat<I, V, E> vertexInputFormat,
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      BspServiceWorker<I, V, E> bspServiceWorker,
      WorkerInputSplitsHandler splitsHandler)  {
    super(context, configuration, bspServiceWorker, splitsHandler);
    this.vertexInputFormat = vertexInputFormat;

    inputSplitMaxVertices = configuration.getInputSplitMaxVertices();
    this.bspServiceWorker = bspServiceWorker;
    vertexInputFilter = configuration.getVertexInputFilter();
    reuseEdgeObjects = configuration.reuseEdgeObjects();
    canEmbedInIds = bspServiceWorker
        .getLocalData()
        .getMappingStoreOps() != null &&
        bspServiceWorker
            .getLocalData()
            .getMappingStoreOps()
            .hasEmbedding();
    translateEdge = bspServiceWorker.getTranslateEdge();

    // Initialize Metrics
    totalVerticesMeter = getTotalVerticesLoadedMeter();
    totalVerticesFilteredCounter = getTotalVerticesFilteredCounter();
    totalEdgesMeter = getTotalEdgesLoadedMeter();
  }

  @Override
  public GiraphInputFormat getInputFormat() {
    return vertexInputFormat;
  }

  @Override
  public InputType getInputType() {
    return InputType.VERTEX;
  }

  /**
   * Read vertices from input split.  If testing, the user may request a
   * maximum number of vertices to be read from an input split.
   *
   * @param inputSplit Input split to process with vertex reader
   * @return Vertices and edges loaded from this input split
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected VertexEdgeCount readInputSplit(
      InputSplit inputSplit) throws IOException, InterruptedException {
    VertexReader<I, V, E> vertexReader =
        vertexInputFormat.createVertexReader(inputSplit, context);
    vertexReader.setConf(configuration);

    WorkerThreadGlobalCommUsage globalCommUsage =
      this.bspServiceWorker
        .getAggregatorHandler().newThreadAggregatorUsage();

    vertexReader.initialize(inputSplit, context);
    // Set aggregator usage to vertex reader
    vertexReader.setWorkerGlobalCommUsage(globalCommUsage);

    long inputSplitVerticesLoaded = 0;
    long inputSplitVerticesFiltered = 0;

    long edgesSinceLastUpdate = 0;
    long inputSplitEdgesLoaded = 0;

    int count = 0;
    OutOfCoreEngine oocEngine = bspServiceWorker.getServerData().getOocEngine();
    while (vertexReader.nextVertex()) {
      // If out-of-core mechanism is used, check whether this thread
      // can stay active or it should temporarily suspend and stop
      // processing and generating more data for the moment.
      if (oocEngine != null &&
          (++count & OutOfCoreEngine.CHECK_IN_INTERVAL) == 0) {
        oocEngine.activeThreadCheckIn();
      }
      Vertex<I, V, E> readerVertex = vertexReader.getCurrentVertex();
      if (readerVertex.getId() == null) {
        throw new IllegalArgumentException(
            "readInputSplit: Vertex reader returned a vertex " +
                "without an id!  - " + readerVertex);
      }
      if (canEmbedInIds) {
        bspServiceWorker
            .getLocalData()
            .getMappingStoreOps()
            .embedTargetInfo(readerVertex.getId());
      }
      if (readerVertex.getValue() == null) {
        readerVertex.setValue(configuration.createVertexValue());
      }
      readerVertex.setConf(configuration);

      ++inputSplitVerticesLoaded;

      if (vertexInputFilter.dropVertex(readerVertex)) {
        ++inputSplitVerticesFiltered;
        if (inputSplitVerticesFiltered % VERTICES_FILTERED_UPDATE_PERIOD == 0) {
          totalVerticesFilteredCounter.inc(inputSplitVerticesFiltered);
          inputSplitVerticesFiltered = 0;
        }
        continue;
      }

      // Before saving to partition-store translate all edges (if present)
      if (translateEdge != null) {
        // only iff vertexInput reads edges also
        if (readerVertex.getEdges() != null && readerVertex.getNumEdges() > 0) {
          OutEdges<I, E> vertexOutEdges = configuration
              .createAndInitializeOutEdges(readerVertex.getNumEdges());
          // TODO : this works for generic OutEdges, can create a better api
          // to support more efficient translation for specific types

          // NOTE : for implementations where edge is reusable, space is
          // consumed by the OutEdges data structure itself, but if not reusable
          // space is consumed by the newly created edge -> and the new OutEdges
          // data structure just holds a reference to the newly created edge
          // so in any way we virtually hold edges twice - similar to
          // OutEdges.trim() -> this has the same complexity as OutEdges.trim()
          for (Edge<I, E> edge : readerVertex.getEdges()) {
            if (reuseEdgeObjects) {
              bspServiceWorker
                  .getLocalData()
                  .getMappingStoreOps()
                  .embedTargetInfo(edge.getTargetVertexId());
              vertexOutEdges.add(edge); // edge can be re-used
            } else { // edge objects cannot be reused - so create new edges
              vertexOutEdges.add(configuration.createEdge(translateEdge, edge));
            }
          }
          // set out edges to translated instance -> old instance is released
          readerVertex.setEdges(vertexOutEdges);
        }
      }

      PartitionOwner partitionOwner =
          bspServiceWorker.getVertexPartitionOwner(readerVertex.getId());
      workerClientRequestProcessor.sendVertexRequest(
          partitionOwner, readerVertex);
      edgesSinceLastUpdate += readerVertex.getNumEdges();

      // Update status every VERTICES_UPDATE_PERIOD vertices
      if (inputSplitVerticesLoaded % VERTICES_UPDATE_PERIOD == 0) {
        totalVerticesMeter.mark(VERTICES_UPDATE_PERIOD);
        WorkerProgress.get().addVerticesLoaded(VERTICES_UPDATE_PERIOD);
        totalEdgesMeter.mark(edgesSinceLastUpdate);
        inputSplitEdgesLoaded += edgesSinceLastUpdate;
        edgesSinceLastUpdate = 0;

        LoggerUtils.setStatusAndLog(
            context, LOG, Level.INFO,
            "readVertexInputSplit: Loaded " +
                totalVerticesMeter.count() + " vertices at " +
                totalVerticesMeter.meanRate() + " vertices/sec " +
                totalEdgesMeter.count() + " edges at " +
                totalEdgesMeter.meanRate() + " edges/sec " +
                MemoryUtils.getRuntimeMemoryStats());
      }

      // For sampling, or to limit outlier input splits, the number of
      // records per input split can be limited
      if (inputSplitMaxVertices > 0 &&
          inputSplitVerticesLoaded >= inputSplitMaxVertices) {
        if (LOG.isInfoEnabled()) {
          LOG.info("readInputSplit: Leaving the input " +
              "split early, reached maximum vertices " +
              inputSplitVerticesLoaded);
        }
        break;
      }
    }

    totalVerticesMeter.mark(inputSplitVerticesLoaded % VERTICES_UPDATE_PERIOD);
    totalEdgesMeter.mark(edgesSinceLastUpdate);
    totalVerticesFilteredCounter.inc(inputSplitVerticesFiltered);

    vertexReader.close();

    WorkerProgress.get().addVerticesLoaded(
        inputSplitVerticesLoaded % VERTICES_UPDATE_PERIOD);
    WorkerProgress.get().incrementVertexInputSplitsLoaded();

    return new VertexEdgeCount(inputSplitVerticesLoaded,
        inputSplitEdgesLoaded + edgesSinceLastUpdate, 0);
  }
}

