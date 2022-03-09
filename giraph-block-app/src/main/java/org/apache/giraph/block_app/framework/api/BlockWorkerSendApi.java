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
package org.apache.giraph.block_app.framework.api;

import java.util.Iterator;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerReduceUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Block computation API available for worker send methods.
 *
 * Interface to the Computation methods.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 */
@SuppressWarnings("rawtypes")
public interface BlockWorkerSendApi<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BlockWorkerApi<I>, WorkerAggregatorUsage, WorkerReduceUsage {
  @Override
  ImmutableClassesGiraphConfiguration<I, V, E> getConf();

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  void sendMessage(I id, M message);

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  void sendMessageToAllEdges(Vertex<I, V, E> vertex, M message);

  /**
   * Send a message to multiple target vertex ids in the iterator.
   *
   * @param vertexIdIterator An iterator to multiple target vertex ids.
   * @param message Message sent to all targets in the iterator.
   */
  void sendMessageToMultipleEdges(Iterator<I> vertexIdIterator, M message);

  /**
   * Sends a request to create a vertex that will be available
   * in the receive phase.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Initial edges
   */
  void addVertexRequest(I id, V value, OutEdges<I, E> edges);

  /**
   * Sends a request to create a vertex that will be available
   * in the receive phase.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  void addVertexRequest(I id, V value);

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next receive phase).
   *
   * @param vertexId Id of the vertex to be removed.
   */
  void removeVertexRequest(I vertexId);

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next receive phase)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   */
  void addEdgeRequest(I sourceVertexId, Edge<I, E> edge);

  /**
   * Request to remove all edges from a given source vertex to a given target
   * vertex (processed just prior to the next receive phase).
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   */
  void removeEdgesRequest(I sourceVertexId, I targetVertexId);
}
