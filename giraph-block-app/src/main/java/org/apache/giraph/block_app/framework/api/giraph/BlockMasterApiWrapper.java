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

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockOutputApi;
import org.apache.giraph.block_app.framework.api.BlockOutputHandleAccessor;
import org.apache.giraph.block_app.framework.api.Counter;
import org.apache.giraph.block_app.framework.internal.BlockCounters;
import org.apache.giraph.block_app.framework.output.BlockOutputDesc;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.block_app.framework.output.BlockOutputWriter;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.internal.ReducersForPieceHandler.BroadcastHandleImpl;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/**
 * Giraph implementation of BlockMasterApi, that delegates all calls
 * to MasterCompute.
 */
final class BlockMasterApiWrapper implements BlockMasterApi,
    BlockOutputApi, BlockOutputHandleAccessor {
  private final MasterCompute master;
  private final BlockOutputHandle outputHandle;

  public BlockMasterApiWrapper(MasterCompute master,
                               BlockOutputHandle outputHandle) {
    this.master = master;
    this.outputHandle = outputHandle;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<?, ?, ?> getConf() {
    return master.getConf();
  }

  @Override
  public void setStatus(String status) {
    master.getContext().setStatus(status);
  }

  @Override
  public void progress() {
    master.getContext().progress();
  }

  @Override
  public Counter getCounter(String group, String name) {
    return BlockCounters.getCounter(master.getContext(), group, name);
  }

  @Override
  public <R extends Writable> R getReduced(String name) {
    return master.getReduced(name);
  }

  @Override
  public void broadcast(String name, Writable value) {
    master.broadcast(name, value);
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp) {
    master.registerReducer(name, reduceOp);
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp, R globalInitialValue) {
    master.registerReducer(name, reduceOp, globalInitialValue);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return master.getAggregatedValue(name);
  }

  @Override
  public <A extends Writable>
  boolean registerAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass
  ) throws InstantiationException, IllegalAccessException {
    return master.registerAggregator(name, aggregatorClass);
  }

  @Override
  public <A extends Writable>
  boolean registerPersistentAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass
  ) throws InstantiationException,
      IllegalAccessException {
    return master.registerPersistentAggregator(name, aggregatorClass);
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    master.setAggregatedValue(name, value);
  }

  @Override
  public <T extends Writable> BroadcastHandle<T> broadcast(T object) {
    BroadcastHandleImpl<T> handle = new BroadcastHandleImpl<>();
    master.broadcast(handle.getName(), object);
    return handle;
  }

  @Override
  @Deprecated
  public long getTotalNumEdges() {
    return master.getTotalNumEdges();
  }

  @Override
  @Deprecated
  public long getTotalNumVertices() {
    return master.getTotalNumVertices();
  }

  @Override
  public void logToCommandLine(String line) {
    master.logToCommandLine(line);
  }

  @Override
  public <OW extends BlockOutputWriter,
      OD extends BlockOutputDesc<OW>> OD getOutputDesc(String confOption) {
    return outputHandle.<OW, OD>getOutputDesc(confOption);
  }

  @Override
  public <OW extends BlockOutputWriter> OW getWriter(String confOption) {
    return outputHandle.getWriter(confOption);
  }

  @Override
  public BlockOutputHandle getBlockOutputHandle() {
    return outputHandle;
  }

  @Override
  public int getWorkerCount() {
    return master.getWorkerInfoList().size();
  }
}
