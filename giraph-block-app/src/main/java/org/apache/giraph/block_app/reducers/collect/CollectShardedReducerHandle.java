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
package org.apache.giraph.block_app.reducers.collect;

import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;

/**
 * ShardedReducerHandle where we keep a list of reduced values
 *
 * @param <S> Single value type
 */
public class CollectShardedReducerHandle<S>
    extends ShardedReducerHandle<S, List<S>> {
  public CollectShardedReducerHandle(CreateReducersApi reduceApi) {
    register(reduceApi);
  }

  @Override
  public ReduceOperation<S, KryoWritableWrapper<List<S>>>
  createReduceOperation() {
    return new CollectReduceOperation<>();
  }

  @Override
  public List<S> createReduceResult(MasterGlobalCommUsage master) {
    int size = 0;
    for (int i = 0; i < REDUCER_COUNT; i++) {
      size += reducers.get(i).getReducedValue(master).get().size();
    }
    return createList(size);
  }

  public List<S> createList(int size) {
    return new ArrayList<S>(size);
  }

  @Override
  public BroadcastHandle<List<S>> createBroadcastHandle(
      BroadcastArrayHandle<KryoWritableWrapper<List<S>>> broadcasts) {
    return new CollectShardedBroadcastHandle(broadcasts);
  }

  /**
   * BroadcastHandle for CollectShardedReducerHandle
   */
  public class CollectShardedBroadcastHandle extends ShardedBroadcastHandle {
    public CollectShardedBroadcastHandle(
        BroadcastArrayHandle<KryoWritableWrapper<List<S>>> broadcasts) {
      super(broadcasts);
    }

    @Override
    public List<S> createBroadcastResult(WorkerBroadcastUsage worker) {
      int size = 0;
      for (int i = 0; i < REDUCER_COUNT; i++) {
        size += broadcasts.get(i).getBroadcast(worker).get().size();
      }
      return createList(size);
    }
  }
}
