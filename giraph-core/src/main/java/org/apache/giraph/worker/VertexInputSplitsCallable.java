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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.yammer.metrics.core.Meter;

import java.io.IOException;

/**
 * Load as many vertex input splits as possible.
 * Every thread will has its own instance of WorkerClientRequestProcessor
 * to send requests.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class VertexInputSplitsCallable<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends InputSplitsCallable<I, V, E, M> {
  /** How often to update metrics and print info */
  public static final int VERTICES_UPDATE_PERIOD = 250000;
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(VertexInputSplitsCallable.class);
  /** Input split max vertices (-1 denotes all) */
  private final long inputSplitMaxVertices;
  /** Bsp service worker (only use thread-safe methods) */
  private final BspServiceWorker<I, V, E, M> bspServiceWorker;

  // Metrics
  /** number of vertices loaded meter across all readers */
  private final Meter totalVerticesMeter;
  /** number of edges loaded meter across all readers */
  private final Meter totalEdgesMeter;

  /**
   * Constructor.
   *
   * @param context Context
   * @param graphState Graph state
   * @param configuration Configuration
   * @param bspServiceWorker service worker
   * @param splitsHandler Handler for input splits
   * @param zooKeeperExt Handle to ZooKeeperExt
   */
  public VertexInputSplitsCallable(
      Mapper<?, ?, ?, ?>.Context context,
      GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      InputSplitsHandler splitsHandler,
      ZooKeeperExt zooKeeperExt)  {
    super(context, graphState, configuration, bspServiceWorker,
        splitsHandler, zooKeeperExt);

    inputSplitMaxVertices = configuration.getInputSplitMaxVertices();
    this.bspServiceWorker = bspServiceWorker;

    // Initialize Metrics
    totalVerticesMeter = getTotalVerticesLoadedMeter();
    totalEdgesMeter = getTotalEdgesLoadedMeter();
  }

  /**
   * Read vertices from input split.  If testing, the user may request a
   * maximum number of vertices to be read from an input split.
   *
   * @param inputSplit Input split to process with vertex reader
   * @param graphState Current graph state
   * @return Vertices and edges loaded from this input split
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected VertexEdgeCount readInputSplit(
      InputSplit inputSplit,
      GraphState<I, V, E, M> graphState)
    throws IOException, InterruptedException {
    VertexInputFormat<I, V, E> vertexInputFormat =
        configuration.createVertexInputFormat();
    VertexReader<I, V, E> vertexReader =
        vertexInputFormat.createVertexReader(inputSplit, context);
    vertexReader.setConf(
        (ImmutableClassesGiraphConfiguration<I, V, E, Writable>) configuration);
    vertexReader.initialize(inputSplit, context);
    long inputSplitVerticesLoaded = 0;
    long edgesSinceLastUpdate = 0;
    long inputSplitEdgesLoaded = 0;
    while (vertexReader.nextVertex()) {
      Vertex<I, V, E, M> readerVertex =
          (Vertex<I, V, E, M>) vertexReader.getCurrentVertex();
      if (readerVertex.getId() == null) {
        throw new IllegalArgumentException(
            "readInputSplit: Vertex reader returned a vertex " +
                "without an id!  - " + readerVertex);
      }
      if (readerVertex.getValue() == null) {
        readerVertex.setValue(configuration.createVertexValue());
      }
      readerVertex.setConf(configuration);
      readerVertex.setGraphState(graphState);

      PartitionOwner partitionOwner =
          bspServiceWorker.getVertexPartitionOwner(readerVertex.getId());
      graphState.getWorkerClientRequestProcessor().sendVertexRequest(
          partitionOwner, readerVertex);
      context.progress(); // do this before potential data transfer
      ++inputSplitVerticesLoaded;
      edgesSinceLastUpdate += readerVertex.getNumEdges();

      // Update status every VERTICES_UPDATE_PERIOD vertices
      if (inputSplitVerticesLoaded % VERTICES_UPDATE_PERIOD == 0) {
        totalVerticesMeter.mark(VERTICES_UPDATE_PERIOD);
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
    vertexReader.close();
    return new VertexEdgeCount(inputSplitVerticesLoaded,
        inputSplitEdgesLoaded + edgesSinceLastUpdate);
  }
}

