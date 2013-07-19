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

package org.apache.giraph.edge;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.collect.MapMaker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects incoming edges for vertices owned by this worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class EdgeStore<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(EdgeStore.class);
  /** Service worker. */
  private CentralizedServiceWorker<I, V, E> service;
  /** Giraph configuration. */
  private ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Progressable to report progress. */
  private Progressable progressable;
  /** Map used to temporarily store incoming edges. */
  private ConcurrentMap<Integer,
      ConcurrentMap<I, OutEdges<I, E>>> transientEdges;
  /**
   * Whether the chosen {@link OutEdges} implementation allows for Edge
   * reuse.
   */
  private boolean reuseEdgeObjects;
  /**
   * Whether the {@link OutEdges} class used during input is different
   * from the one used during computation.
   */
  private boolean useInputOutEdges;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public EdgeStore(
      CentralizedServiceWorker<I, V, E> service,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      Progressable progressable) {
    this.service = service;
    this.configuration = configuration;
    this.progressable = progressable;
    transientEdges = new MapMaker().concurrencyLevel(
        configuration.getNettyServerExecutionConcurrency()).makeMap();
    reuseEdgeObjects = configuration.reuseEdgeObjects();
    useInputOutEdges = configuration.useInputOutEdges();
  }

  /**
   * Add edges belonging to a given partition on this worker.
   * Note: This method is thread-safe.
   *
   * @param partitionId Partition id for the incoming edges.
   * @param edges Incoming edges
   */
  public void addPartitionEdges(
      int partitionId, ByteArrayVertexIdEdges<I, E> edges) {
    ConcurrentMap<I, OutEdges<I, E>> partitionEdges =
        transientEdges.get(partitionId);
    if (partitionEdges == null) {
      ConcurrentMap<I, OutEdges<I, E>> newPartitionEdges =
          new MapMaker().concurrencyLevel(
              configuration.getNettyServerExecutionConcurrency()).makeMap();
      partitionEdges = transientEdges.putIfAbsent(partitionId,
          newPartitionEdges);
      if (partitionEdges == null) {
        partitionEdges = newPartitionEdges;
      }
    }
    ByteArrayVertexIdEdges<I, E>.VertexIdEdgeIterator vertexIdEdgeIterator =
        edges.getVertexIdEdgeIterator();
    while (vertexIdEdgeIterator.hasNext()) {
      vertexIdEdgeIterator.next();
      I vertexId = vertexIdEdgeIterator.getCurrentVertexId();
      Edge<I, E> edge = reuseEdgeObjects ?
          vertexIdEdgeIterator.getCurrentEdge() :
          vertexIdEdgeIterator.releaseCurrentEdge();
      OutEdges<I, E> outEdges = partitionEdges.get(vertexId);
      if (outEdges == null) {
        OutEdges<I, E> newOutEdges =
            configuration.createAndInitializeInputOutEdges();
        outEdges = partitionEdges.putIfAbsent(vertexId, newOutEdges);
        if (outEdges == null) {
          outEdges = newOutEdges;
          // Since we had to use the vertex id as a new key in the map,
          // we need to release the object.
          vertexIdEdgeIterator.releaseCurrentVertexId();
        }
      }
      synchronized (outEdges) {
        outEdges.add(edge);
      }
    }
  }

  /**
   * Convert the input edges to the {@link OutEdges} data structure used
   * for computation (if different).
   *
   * @param inputEdges Input edges
   * @return Compute edges
   */
  private OutEdges<I, E> convertInputToComputeEdges(
      OutEdges<I, E> inputEdges) {
    if (!useInputOutEdges) {
      return inputEdges;
    } else {
      return configuration.createAndInitializeOutEdges(inputEdges);
    }
  }

  /**
   * Move all edges from temporary storage to their source vertices.
   * Note: this method is not thread-safe.
   */
  public void moveEdgesToVertices() {
    if (transientEdges.isEmpty()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("moveEdgesToVertices: No edges to move");
      }
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Moving incoming edges to vertices.");
    }

    final BlockingQueue<Integer> partitionIdQueue =
        new ArrayBlockingQueue<Integer>(transientEdges.size());
    partitionIdQueue.addAll(transientEdges.keySet());
    int numThreads = configuration.getNumInputSplitsThreads();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Integer partitionId;
            while ((partitionId = partitionIdQueue.poll()) != null) {
              Partition<I, V, E> partition =
                  service.getPartitionStore().getOrCreatePartition(partitionId);
              ConcurrentMap<I, OutEdges<I, E>> partitionEdges =
                  transientEdges.remove(partitionId);
              for (I vertexId : partitionEdges.keySet()) {
                OutEdges<I, E> outEdges = convertInputToComputeEdges(
                    partitionEdges.remove(vertexId));
                Vertex<I, V, E> vertex = partition.getVertex(vertexId);
                // If the source vertex doesn't exist, create it. Otherwise,
                // just set the edges.
                if (vertex == null) {
                  vertex = configuration.createVertex();
                  vertex.initialize(vertexId, configuration.createVertexValue(),
                      outEdges);
                  partition.putVertex(vertex);
                } else {
                  // A vertex may exist with or without edges initially
                  // and optimize the case of no initial edges
                  if (vertex.getNumEdges() == 0) {
                    vertex.setEdges(outEdges);
                  } else {
                    for (Edge<I, E> edge : outEdges) {
                      vertex.addEdge(edge);
                    }
                  }
                  // Some Partition implementations (e.g. ByteArrayPartition)
                  // require us to put back the vertex after modifying it.
                  partition.saveVertex(vertex);
                }
              }
              // Some PartitionStore implementations
              // (e.g. DiskBackedPartitionStore) require us to put back the
              // partition after modifying it.
              service.getPartitionStore().putPartition(partition);
            }
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "move-edges-%d", progressable);

    transientEdges.clear();

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Finished moving incoming edges to " +
          "vertices.");
    }
  }
}
