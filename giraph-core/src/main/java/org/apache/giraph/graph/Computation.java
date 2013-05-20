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

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Basic abstract class for writing a BSP application for computation.
 *
 * During the superstep there can be several instances of this class,
 * each doing computation on one partition of the graph's vertices.
 *
 * Note that each thread will have its own {@link Computation},
 * so accessing any data from this class is thread-safe.
 * However, accessing global data (like data from {@link WorkerContext})
 * is not thread-safe.
 *
 * Objects of this class only live for a single superstep.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public abstract class Computation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements WorkerAggregatorUsage {
  /** Global graph state **/
  private GraphState graphState;
  /** Handles requests */
  private WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor;
  /** Graph-wide BSP Mapper for this Computation */
  private GraphTaskManager<I, V, E> graphTaskManager;
  /** Worker aggregator usage */
  private WorkerAggregatorUsage workerAggregatorUsage;
  /** Worker context */
  private WorkerContext workerContext;

  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   *                 a life expectancy as long as next() is not called.
   */
  public abstract void compute(Vertex<I, V, E> vertex,
      Iterable<M1> messages) throws IOException;

  /**
   * Prepare for computation. This method is executed exactly once prior to
   * {@link #compute(Vertex, Iterable)} being called for any of the vertices
   * in the partition.
   */
  public void preSuperstep() {
  }

  /**
   * Finish computation. This method is executed exactly once after computation
   * for all vertices in the partition is complete.
   */
  public void postSuperstep() {
  }

  /**
   * Initialize, called by infrastructure before the superstep starts.
   * Shouldn't be called by user code.
   *
   * @param graphState Graph state
   * @param workerClientRequestProcessor Processor for handling requests
   * @param graphTaskManager Graph-wide BSP Mapper for this Vertex
   * @param workerAggregatorUsage Worker aggregator usage
   * @param workerContext Worker context
   */
  public final void initialize(
      GraphState graphState,
      WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
      GraphTaskManager<I, V, E> graphTaskManager,
      WorkerAggregatorUsage workerAggregatorUsage,
      WorkerContext workerContext) {
    this.graphState = graphState;
    this.workerClientRequestProcessor = workerClientRequestProcessor;
    this.graphTaskManager = graphTaskManager;
    this.workerAggregatorUsage = workerAggregatorUsage;
    this.workerContext = workerContext;
  }

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  public long getSuperstep() {
    return graphState.getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public long getTotalNumVertices() {
    return graphState.getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public long getTotalNumEdges() {
    return graphState.getTotalNumEdges();
  }

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  public void sendMessage(I id, M2 message) {
    if (workerClientRequestProcessor.sendMessageRequest(id, message)) {
      graphTaskManager.notifySentMessages();
    }
  }

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    for (Edge<I, E> edge : vertex.getEdges()) {
      sendMessage(edge.getTargetVertexId(), message);
    }
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Initial edges
   */
  public void addVertexRequest(I id, V value,
      OutEdges<I, E> edges) throws IOException {
    Vertex<I, V, E> vertex = getConf().createVertex();
    vertex.initialize(id, value, edges);
    workerClientRequestProcessor.addVertexRequest(vertex);
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  public void addVertexRequest(I id, V value) throws IOException {
    addVertexRequest(id, value, getConf().createAndInitializeOutEdges());
  }

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param vertexId Id of the vertex to be removed.
   */
  public void removeVertexRequest(I vertexId) throws IOException {
    workerClientRequestProcessor.removeVertexRequest(vertexId);
  }

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next superstep)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   */
  public void addEdgeRequest(I sourceVertexId,
      Edge<I, E> edge) throws IOException {
    workerClientRequestProcessor.addEdgeRequest(sourceVertexId, edge);
  }

  /**
   * Request to remove all edges from a given source vertex to a given target
   * vertex (processed just prior to the next superstep).
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   */
  public void removeEdgesRequest(I sourceVertexId,
      I targetVertexId) throws IOException {
    workerClientRequestProcessor.removeEdgesRequest(
        sourceVertexId, targetVertexId);
  }

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  public Mapper.Context getContext() {
    return graphState.getContext();
  }

  /**
   * Get the worker context
   *
   * @param <W> WorkerContext class
   * @return WorkerContext context
   */
  @SuppressWarnings("unchecked")
  public <W extends WorkerContext> W getWorkerContext() {
    return (W) workerContext;
  }

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    workerAggregatorUsage.aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return workerAggregatorUsage.<A>getAggregatedValue(name);
  }
}
