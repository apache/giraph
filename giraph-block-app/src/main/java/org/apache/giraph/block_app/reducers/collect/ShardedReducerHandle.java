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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.reducers.array.ArrayOfHandles;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.giraph.writable.kryo.TransientRandom;

/**
 * Reducing values into a list of reducers, randomly,
 * and getting the results of all reducers together
 *
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public abstract class ShardedReducerHandle<S, R>
    implements ReducerHandle<S, R> {
  // Use a prime number for number of reducers, large enough to make sure
  // request sizes are within expected size (0.5MB)
  protected static final int REDUCER_COUNT = 39989;

  protected final TransientRandom random = new TransientRandom();

  protected ArrayOfHandles.ArrayOfReducers<S, KryoWritableWrapper<R>> reducers;

  public final void register(final CreateReducersApi reduceApi) {
    reducers = new ArrayOfHandles.ArrayOfReducers<>(REDUCER_COUNT,
        new Supplier<ReducerHandle<S, KryoWritableWrapper<R>>>() {
          @Override
          public ReducerHandle<S, KryoWritableWrapper<R>> get() {
            return reduceApi.createLocalReducer(createReduceOperation());
          }
        });
  }

  @Override
  public final void reduce(S value) {
    reducers.get(random.nextInt(REDUCER_COUNT)).reduce(value);
  }

  @Override
  public final R getReducedValue(MasterGlobalCommUsage master) {
    KryoWritableWrapper<R> ret = new KryoWritableWrapper<>(
        createReduceResult(master));
    ReduceOperation<S, KryoWritableWrapper<R>> reduceOperation =
        createReduceOperation();
    for (int i = 0; i < REDUCER_COUNT; i++) {
      reduceOperation.reduceMerge(ret,
          reducers.get(i).getReducedValue(master));
    }
    return ret.get();
  }

  public abstract ReduceOperation<S, KryoWritableWrapper<R>>
  createReduceOperation();

  public R createReduceResult(MasterGlobalCommUsage master) {
    return createReduceOperation().createInitialValue().get();
  }

  public BroadcastHandle<R> createBroadcastHandle(
      BroadcastArrayHandle<KryoWritableWrapper<R>> broadcasts) {
    return new ShardedBroadcastHandle(broadcasts);
  }

  @Override
  public final BroadcastHandle<R> broadcastValue(BlockMasterApi masterApi) {
    return createBroadcastHandle(reducers.broadcastValue(masterApi));
  }

  /**
   * Broadcast for ShardedReducerHandle
   */
  public class ShardedBroadcastHandle implements BroadcastHandle<R> {
    protected final BroadcastArrayHandle<KryoWritableWrapper<R>> broadcasts;

    public ShardedBroadcastHandle(
        BroadcastArrayHandle<KryoWritableWrapper<R>> broadcasts) {
      this.broadcasts = broadcasts;
    }

    public R createBroadcastResult(WorkerBroadcastUsage worker) {
      return createReduceOperation().createInitialValue().get();
    }

    @Override
    public final R getBroadcast(WorkerBroadcastUsage worker) {
      KryoWritableWrapper<R> ret = new KryoWritableWrapper<>(
          createBroadcastResult(worker));
      ReduceOperation<S, KryoWritableWrapper<R>> reduceOperation =
          createReduceOperation();
      for (int i = 0; i < REDUCER_COUNT; i++) {
        reduceOperation.reduceMerge(ret,
            broadcasts.get(i).getBroadcast(worker));
      }
      return ret.get();
    }
  }
}
