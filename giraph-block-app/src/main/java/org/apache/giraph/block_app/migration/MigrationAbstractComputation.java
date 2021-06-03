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
package org.apache.giraph.block_app.migration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorDelegator;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Replacement for AbstractComputation when migrating to
 * Blocks Framework, disallowing functions that are tied to
 * execution order.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 * @param <M1> Incoming Message Value
 * @param <M2> Outgoing Message Value
 */
@SuppressWarnings("rawtypes")
public class MigrationAbstractComputation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable> extends WorkerAggregatorDelegator<I, V, E>
    implements TypesHolder<I, V, E, M1, M2>, Writable {
  private BlockWorkerSendApi<I, V, E, M2> api;
  private MigrationWorkerContext workerContext;
  private long superstep;

  final void init(
      BlockWorkerSendApi<I, V, E, M2> workerApi,
      MigrationWorkerContext workerContext,
      long superstep) {
    this.api = workerApi;
    this.workerContext = workerContext;
    this.superstep = superstep;
    setWorkerGlobalCommUsage((WorkerGlobalCommUsage) workerApi);
    setConf(workerApi.getConf());
  }

  public void compute(Vertex<I, V, E> vertex,
      Iterable<M1> messages) throws IOException {
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  public void preSuperstep() {
  }

  public void postSuperstep() {
  }

  @SuppressWarnings("deprecation")
  public long getTotalNumVertices() {
    return api.getTotalNumVertices();
  }

  @SuppressWarnings("deprecation")
  public long getTotalNumEdges() {
    return api.getTotalNumEdges();
  }

  public void sendMessage(I id, M2 message) {
    api.sendMessage(id, message);
  }

  public final void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    api.sendMessageToAllEdges(vertex, message);
  }

  public final void sendMessageToMultipleEdges(
      Iterator<I> vertexIdIterator, M2 message) {
    api.sendMessageToMultipleEdges(vertexIdIterator, message);
  }

  public final void addVertexRequest(I id, V value,
      OutEdges<I, E> edges) throws IOException {
    api.addVertexRequest(id, value, edges);
  }

  public final void addVertexRequest(I id, V value) throws IOException {
    api.addVertexRequest(id, value);
  }

  public final void removeVertexRequest(I vertexId) throws IOException {
    api.removeVertexRequest(vertexId);
  }

  public final void addEdgeRequest(I sourceVertexId,
      Edge<I, E> edge) throws IOException {
    api.addEdgeRequest(sourceVertexId, edge);
  }

  public final void removeEdgesRequest(I sourceVertexId,
      I targetVertexId) throws IOException {
    api.removeEdgesRequest(sourceVertexId, targetVertexId);
  }

  @SuppressWarnings("unchecked")
  public <W extends MigrationWorkerContext> W getWorkerContext() {
    return (W) workerContext;
  }

  /**
   * Drop-in replacement for BasicComputation when migrating to
   * Blocks Framework, disallowing functions that are tied to
   * execution order.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param <M> Message type
   */
  public static class MigrationBasicComputation<I extends WritableComparable,
      V extends Writable, E extends Writable, M extends Writable>
      extends MigrationAbstractComputation<I, V, E, M, M> {
  }

  /**
   * Drop-in replacement for AbstractComputation when migrating to
   * Blocks Framework.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param <M1> Incoming Message Value
   * @param <M2> Outgoing Message Value
   */
  public static class MigrationFullAbstractComputation
      <I extends WritableComparable, V extends Writable, E extends Writable,
      M1 extends Writable, M2 extends Writable>
      extends MigrationAbstractComputation<I, V, E, M1, M2> {
    public long getSuperstep() {
      return super.superstep;
    }
  }

  /**
   * Drop-in replacement for BasicComputation when migrating to
   * Blocks Framework.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param <M> Message type
   */
  public static class MigrationFullBasicComputation
      <I extends WritableComparable, V extends Writable, E extends Writable,
      M extends Writable>
      extends MigrationFullAbstractComputation<I, V, E, M, M> {
  }
}
