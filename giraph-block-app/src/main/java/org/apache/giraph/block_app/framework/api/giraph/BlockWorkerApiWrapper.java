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
package org.apache.giraph.block_app.framework.api.giraph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockOutputApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerValueAccessor;
import org.apache.giraph.block_app.framework.api.Counter;
import org.apache.giraph.block_app.framework.internal.BlockCounters;
import org.apache.giraph.block_app.framework.output.BlockOutputDesc;
import org.apache.giraph.block_app.framework.output.BlockOutputWriter;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Giraph implementation of BlockWorkerReceiveApi and BlockWorkerSendAPI,
 * passing all calls to Computation.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 */
@SuppressWarnings("rawtypes")
final class BlockWorkerApiWrapper<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements BlockWorkerReceiveApi<I>, BlockWorkerSendApi<I, V, E, M>,
    BlockWorkerValueAccessor, WorkerGlobalCommUsage, BlockOutputApi {
  private final Computation<I, V, E, NoMessage, M> worker;

  public BlockWorkerApiWrapper(Computation<I, V, E, NoMessage, M> worker) {
    this.worker = worker;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
    return worker.getConf();
  }

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    worker.aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return worker.getAggregatedValue(name);
  }

  @Override
  public <B extends Writable> B getBroadcast(String name) {
    return worker.getBroadcast(name);
  }

  @Override
  public void reduce(String name, Object value) {
    worker.reduce(name, value);
  }

  @Override
  public void reduceMerge(String name, Writable value) {
    worker.reduceMerge(name, value);
  }

  @Override
  public void sendMessage(I id, M message) {
    worker.sendMessage(id, message);
  }

  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M message) {
    worker.sendMessageToAllEdges(vertex, message);
  }

  @Override
  public void sendMessageToMultipleEdges(
      Iterator<I> vertexIdIterator, M message) {
    worker.sendMessageToMultipleEdges(vertexIdIterator, message);
  }

  @Override
  public void addVertexRequest(I id, V value) {
    try {
      worker.addVertexRequest(id, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addVertexRequest(I id, V value, OutEdges<I, E> edges) {
    try {
      worker.addVertexRequest(id, value, edges);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeVertexRequest(I vertexId) {
    try {
      worker.removeVertexRequest(vertexId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) {
    try {
      worker.addEdgeRequest(sourceVertexId, edge);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeEdgesRequest(I sourceVertexId, I targetVertexId) {
    try {
      worker.removeEdgesRequest(sourceVertexId, targetVertexId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BlockWorkerContext getBlockWorkerContext() {
    return (BlockWorkerContext) worker.getWorkerContext();
  }

  @Override
  public Object getWorkerValue() {
    return getBlockWorkerContext().getWorkerValue();
  }

  @Override
  public long getTotalNumEdges() {
    return worker.getTotalNumEdges();
  }

  @Override
  public long getTotalNumVertices() {
    return worker.getTotalNumVertices();
  }

  @Override
  public <OW extends BlockOutputWriter, OD extends BlockOutputDesc<OW>>
  OD getOutputDesc(String confOption) {
    return getBlockWorkerContext().getOutputHandle().<OW, OD>getOutputDesc(
        confOption);
  }

  @Override
  public <OW extends BlockOutputWriter> OW getWriter(String confOption) {
    return getBlockWorkerContext().getOutputHandle().getWriter(confOption);
  }

  @Override
  public int getMyWorkerIndex() {
    return worker.getMyWorkerIndex();
  }

  @Override
  public int getWorkerCount() {
    return worker.getWorkerCount();
  }

  @Override
  public int getWorkerForVertex(I vertexId) {
    return worker.getWorkerForVertex(vertexId);
  }

  @Override
  public Counter getCounter(String group, String name) {
    return BlockCounters.getCounter(worker.getContext(), group, name);
  }

  @Override
  public void progress() {
    worker.getContext().progress();
  }

  @Override
  public void setStatus(String status) {
    worker.getContext().setStatus(status);
  }
}
