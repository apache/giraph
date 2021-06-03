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

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.worker.AllWorkersInfo;
import org.apache.giraph.worker.WorkerAggregatorDelegator;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * See {@link Computation} for explanation of the interface.
 *
 * This is a abstract class helper for users to implement their computations.
 * It implements all of the methods required by the {@link Computation}
 * interface except for the {@link #compute(Vertex, Iterable)} which we leave
 * to the user to define.
 *
 * In most cases users should inherit from this class when implementing their
 * algorithms with Giraph.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public abstract class AbstractComputation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable>
    extends WorkerAggregatorDelegator<I, V, E>
    implements Computation<I, V, E, M1, M2> {
  /** Global graph state **/
  private GraphState graphState;
  /** Handles requests */
  private WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor;
  /** Service worker */
  private CentralizedServiceWorker<I, V, E> serviceWorker;
  /** Worker context */
  private WorkerContext workerContext;
  /** All workers info */
  private AllWorkersInfo allWorkersInfo;

  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   *                 a life expectancy as long as next() is not called.
   */
  @Override
  public abstract void compute(Vertex<I, V, E> vertex,
      Iterable<M1> messages) throws IOException;

  /**
   * Prepare for computation. This method is executed exactly once prior to
   * {@link #compute(Vertex, Iterable)} being called for any of the vertices
   * in the partition.
   */
  @Override
  public void preSuperstep() {
  }

  /**
   * Finish computation. This method is executed exactly once after computation
   * for all vertices in the partition is complete.
   */
  @Override
  public void postSuperstep() {
  }

  /**
   * Initialize, called by infrastructure before the superstep starts.
   * Shouldn't be called by user code.
   *
   * @param graphState Graph state
   * @param workerClientRequestProcessor Processor for handling requests
   * @param serviceWorker Graph-wide BSP Mapper for this Vertex
   * @param workerGlobalCommUsage Worker global communication usage
   */
  @Override
  public void initialize(
      GraphState graphState,
      WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
      CentralizedServiceWorker<I, V, E> serviceWorker,
      WorkerGlobalCommUsage workerGlobalCommUsage) {
    this.graphState = graphState;
    this.workerClientRequestProcessor = workerClientRequestProcessor;
    this.setWorkerGlobalCommUsage(workerGlobalCommUsage);
    this.serviceWorker = serviceWorker;
    if (serviceWorker != null) {
      this.workerContext = serviceWorker.getWorkerContext();
      this.allWorkersInfo = new AllWorkersInfo(
          serviceWorker.getWorkerInfoList(), serviceWorker.getWorkerInfo());
    } else {
      this.workerContext = null;
      this.allWorkersInfo = null;
    }
  }

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  @Override
  public long getSuperstep() {
    return graphState.getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  @Override
  public long getTotalNumVertices() {
    return graphState.getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  @Override
  public long getTotalNumEdges() {
    return graphState.getTotalNumEdges();
  }

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  @Override
  public void sendMessage(I id, M2 message) {
    workerClientRequestProcessor.sendMessageRequest(id, message);
  }

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    workerClientRequestProcessor.sendMessageToAllRequest(vertex, message);
  }

  /**
   * Send a message to multiple target vertex ids in the iterator.
   *
   * @param vertexIdIterator An iterator to multiple target vertex ids.
   * @param message Message sent to all targets in the iterator.
   */
  @Override
  public void sendMessageToMultipleEdges(
      Iterator<I> vertexIdIterator, M2 message) {
    workerClientRequestProcessor.sendMessageToAllRequest(
        vertexIdIterator, message);
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Initial edges
   */
  @Override
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
  @Override
  public void addVertexRequest(I id, V value) throws IOException {
    addVertexRequest(id, value, getConf().createAndInitializeOutEdges());
  }

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param vertexId Id of the vertex to be removed.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public <W extends WorkerContext> W getWorkerContext() {
    return (W) workerContext;
  }

  @Override
  public final int getWorkerCount() {
    return allWorkersInfo.getWorkerCount();
  }

  @Override
  public final int getMyWorkerIndex() {
    return allWorkersInfo.getMyWorkerIndex();
  }

  @Override
  public final int getWorkerForVertex(I vertexId) {
    return allWorkersInfo.getWorkerIndex(
        serviceWorker.getVertexPartitionOwner(vertexId).getWorkerInfo());
  }
}
