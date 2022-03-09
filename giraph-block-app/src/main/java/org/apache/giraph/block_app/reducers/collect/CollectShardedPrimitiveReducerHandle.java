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

import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;

/**
 * ShardedReducerHandle where we keep a list of reduced values,
 * when primitives are used
 *
 * @param <S> Single value type
 */
public class CollectShardedPrimitiveReducerHandle<S>
    extends ShardedReducerHandle<S, WArrayList<S>> {
  /**
   * Type ops if available, or null
   */
  private final PrimitiveTypeOps<S> typeOps;

  public CollectShardedPrimitiveReducerHandle(final CreateReducersApi reduceApi,
      Class<S> valueClass) {
    typeOps = TypeOpsUtils.getPrimitiveTypeOps(valueClass);
    register(reduceApi);
  }

  @Override
  public ReduceOperation<S, KryoWritableWrapper<WArrayList<S>>>
  createReduceOperation() {
    return new CollectPrimitiveReduceOperation<>(typeOps);
  }

  @Override
  public WArrayList<S> createReduceResult(MasterGlobalCommUsage master) {
    int size = 0;
    for (int i = 0; i < REDUCER_COUNT; i++) {
      size += reducers.get(i).getReducedValue(master).get().size();
    }
    return createList(size);
  }

  public WArrayList<S> createList(int size) {
    return typeOps.createArrayList(size);
  }

  @Override
  public BroadcastHandle<WArrayList<S>> createBroadcastHandle(
      BroadcastArrayHandle<KryoWritableWrapper<WArrayList<S>>> broadcasts) {
    return new CollectShardedPrimitiveBroadcastHandle(broadcasts);
  }

  /**
   * Broadcast handle for CollectShardedPrimitiveReducerHandle
   */
  public class CollectShardedPrimitiveBroadcastHandle
      extends ShardedBroadcastHandle {
    public CollectShardedPrimitiveBroadcastHandle(
        BroadcastArrayHandle<KryoWritableWrapper<WArrayList<S>>>
            broadcasts) {
      super(broadcasts);
    }

    @Override
    public WArrayList<S> createBroadcastResult(
        WorkerBroadcastUsage worker) {
      int size = 0;
      for (int i = 0; i < REDUCER_COUNT; i++) {
        size += broadcasts.get(i).getBroadcast(worker).get().size();
      }
      return createList(size);
    }
  }
}
