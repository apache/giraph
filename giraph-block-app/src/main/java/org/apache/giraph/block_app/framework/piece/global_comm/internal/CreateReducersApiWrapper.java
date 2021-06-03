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
package org.apache.giraph.block_app.framework.piece.global_comm.internal;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/**
 * Wrapping masterApi and reducers handler into API for creating reducer
 * handles.
 */
public class CreateReducersApiWrapper implements CreateReducersApi {
  private final BlockMasterApi masterApi;
  private final ReducersForPieceHandler reducersApi;

  public CreateReducersApiWrapper(
      BlockMasterApi masterApi, ReducersForPieceHandler reducersApi) {
    this.masterApi = masterApi;
    this.reducersApi = reducersApi;
  }

  @Override
  public <S, R extends Writable> ReducerHandle<S, R> createLocalReducer(
      ReduceOperation<S, R> reduceOp) {
    return reducersApi.createLocalReducer(
        masterApi, reduceOp, reduceOp.createInitialValue());
  }

  @Override
  public <S, R extends Writable> ReducerHandle<S, R> createLocalReducer(
      ReduceOperation<S, R> reduceOp, R globalInitialValue) {
    return reducersApi.createLocalReducer(
        masterApi, reduceOp, globalInitialValue);
  }

  @Override
  public <S, R extends Writable> ReducerHandle<S, R> createGlobalReducer(
      ReduceOperation<S, R> reduceOp) {
    return reducersApi.createGlobalReducer(
        masterApi, reduceOp, reduceOp.createInitialValue());
  }

  @Override
  public <S, R extends Writable> ReducerHandle<S, R> createGlobalReducer(
      ReduceOperation<S, R> reduceOp, R globalInitialValue) {
    return reducersApi.createGlobalReducer(
        masterApi, reduceOp, globalInitialValue);
  }

  @Override
  public ImmutableClassesGiraphConfiguration<?, ?, ?> getConf() {
    return masterApi.getConf();
  }
}
