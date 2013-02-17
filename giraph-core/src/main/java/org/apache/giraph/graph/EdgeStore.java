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

package org.apache.giraph.graph;

import com.google.common.collect.MapMaker;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ByteArrayEdges;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.vertex.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects incoming edges for vertices owned by this worker.
 * Note: the current implementation is simply a bridge between
 * incoming requests and vertices. In the future, EdgeStore may become an
 * interface allowing for alternative, pluggable implementations of edge
 * storage without having to extend Vertex.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class EdgeStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(EdgeStore.class);
  /** Service worker. */
  private CentralizedServiceWorker<I, V, E, M> service;
  /** Giraph configuration. */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** Progressable to report progress. */
  private Progressable progressable;
  /** Map used to temporarily store incoming edges. */
  private ConcurrentMap<Integer,
      ConcurrentMap<I, ByteArrayEdges<I, E>>> transientEdges;
  /**
   * Whether we should reuse edge objects (cached to avoid expensive calls
   * to the configuration).
   */
  private boolean reuseIncomingEdgeObjects;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public EdgeStore(
      CentralizedServiceWorker<I, V, E, M> service,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      Progressable progressable) {
    this.service = service;
    this.configuration = configuration;
    this.progressable = progressable;
    reuseIncomingEdgeObjects = configuration.reuseIncomingEdgeObjects();
    transientEdges = new MapMaker().concurrencyLevel(
        configuration.getNettyServerExecutionConcurrency()).makeMap();
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
    ConcurrentMap<I, ByteArrayEdges<I, E>> partitionEdges =
        transientEdges.get(partitionId);
    if (partitionEdges == null) {
      ConcurrentMap<I, ByteArrayEdges<I, E>> newPartitionEdges =
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
      Edge<I, E> edge = vertexIdEdgeIterator.getCurrentEdge();
      ByteArrayEdges<I, E> vertexEdges = partitionEdges.get(vertexId);
      if (vertexEdges == null) {
        ByteArrayEdges<I, E> newVertexEdges =
            new ByteArrayEdges<I, E>(configuration);
        vertexEdges = partitionEdges.putIfAbsent(vertexId, newVertexEdges);
        if (vertexEdges == null) {
          vertexEdges = newVertexEdges;
          // Since we had to use the vertex id as a new key in the map,
          // we need to release the object.
          vertexIdEdgeIterator.releaseCurrentVertexId();
        }
      }
      synchronized (vertexEdges) {
        vertexEdges.appendEdge(edge);
      }
    }
  }

  /**
   * Move all edges from temporary storage to their source vertices.
   * Note: this method is not thread-safe.
   */
  public void moveEdgesToVertices() {
    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Moving incoming edges to vertices.");
    }
    for (Map.Entry<Integer, ConcurrentMap<I,
        ByteArrayEdges<I, E>>> partitionEdges : transientEdges.entrySet()) {
      Partition<I, V, E, M> partition =
          service.getPartitionStore().getPartition(partitionEdges.getKey());
      for (I vertexId : partitionEdges.getValue().keySet()) {
        // Depending on whether the vertex implementation keeps references to
        // the Edge objects or not, we may be able to reuse objects when
        // iterating.
        Iterable<Edge<I, E>> edgesIterable = reuseIncomingEdgeObjects ?
            partitionEdges.getValue().remove(vertexId) :
            partitionEdges.getValue().remove(vertexId).copyEdgeIterable();
        Vertex<I, V, E, M> vertex = partition.getVertex(vertexId);
        // If the source vertex doesn't exist, create it. Otherwise,
        // just set the edges.
        if (vertex == null) {
          vertex = configuration.createVertex();
          vertex.initialize(vertexId, configuration.createVertexValue(),
              edgesIterable);
          partition.putVertex(vertex);
        } else {
          vertex.setEdges(edgesIterable);
          // Some Partition implementations (e.g. ByteArrayPartition) require
          // us to put back the vertex after modifying it.
          partition.saveVertex(vertex);
        }
        progressable.progress();
      }
      // Some PartitionStore implementations (e.g. DiskBackedPartitionStore)
      // require us to put back the partition after modifying it.
      service.getPartitionStore().putPartition(partition);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Finished moving incoming edges to " +
          "vertices.");
    }
    transientEdges.clear();
  }
}
