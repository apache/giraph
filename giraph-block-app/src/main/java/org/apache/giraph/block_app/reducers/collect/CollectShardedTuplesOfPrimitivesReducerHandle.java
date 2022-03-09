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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
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
 * and values consist of multiple primitives, so we keep one primitive
 * list for each
 */
@SuppressWarnings("unchecked")
public class CollectShardedTuplesOfPrimitivesReducerHandle
extends ShardedReducerHandle<List<Object>, List<WArrayList>> {
  /**
   * Type ops if available, or null
   */
  private final List<PrimitiveTypeOps> typeOpsList;

  public CollectShardedTuplesOfPrimitivesReducerHandle(
      final CreateReducersApi reduceApi, Class<?>... valueClasses) {
    typeOpsList = new ArrayList<>();
    for (Class<?> valueClass : valueClasses) {
      typeOpsList.add(TypeOpsUtils.getPrimitiveTypeOps(valueClass));
    }
    register(reduceApi);
  }

  public List<Object> createSingleValue() {
    List<Object> ret = new ArrayList<>();
    for (PrimitiveTypeOps typeOps : typeOpsList) {
      ret.add(typeOps.create());
    }
    return ret;
  }

  @Override
  public ReduceOperation<List<Object>,
      KryoWritableWrapper<List<WArrayList>>> createReduceOperation() {
    return new CollectTuplesOfPrimitivesReduceOperation(typeOpsList);
  }

  @Override
  public List<WArrayList> createReduceResult(
      MasterGlobalCommUsage master) {
    int size = 0;
    for (int i = 0; i < REDUCER_COUNT; i++) {
      size += reducers.get(i).getReducedValue(master).get().get(0).size();
    }
    return createLists(size);
  }

  public List<WArrayList> createLists(int size) {
    List<WArrayList> ret = new ArrayList<>();
    for (PrimitiveTypeOps typeOps : typeOpsList) {
      ret.add(typeOps.createArrayList(size));
    }
    return ret;
  }

  @Override
  public BroadcastHandle<List<WArrayList>> createBroadcastHandle(
      BroadcastArrayHandle<KryoWritableWrapper<List<WArrayList>>>
          broadcasts) {
    return new CollectShardedTuplesOfPrimitivesBroadcastHandle(broadcasts);
  }

  /**
   * BroadcastHandle for CollectShardedTuplesOfPrimitivesReducerHandle
   */
  public class CollectShardedTuplesOfPrimitivesBroadcastHandle
      extends ShardedBroadcastHandle {
    public CollectShardedTuplesOfPrimitivesBroadcastHandle(
        BroadcastArrayHandle<KryoWritableWrapper<List<WArrayList>>>
            broadcasts) {
      super(broadcasts);
    }

    @Override
    public List<WArrayList> createBroadcastResult(
        WorkerBroadcastUsage worker) {
      int size = 0;
      for (int i = 0; i < REDUCER_COUNT; i++) {
        size += broadcasts.get(i).getBroadcast(worker).get().size();
      }
      return createLists(size);
    }
  }

  /**
   * Reduce broadcast wrapper
   */
  public static class CollectShardedTuplesOfPrimitivesReduceBroadcast {
    private CollectShardedTuplesOfPrimitivesReducerHandle reducerHandle;
    private BroadcastHandle<List<WArrayList>> broadcastHandle;

    /** Set reducer handle to just registered handle */
    public void registeredReducer(CreateReducersApi reduceApi,
        Class<?>... valueClasses) {
      this.reducerHandle = new CollectShardedTuplesOfPrimitivesReducerHandle(
          reduceApi, valueClasses);
    }

    public List<Object> createSingleValue() {
      return reducerHandle.createSingleValue();
    }

    /** Reduce single value */
    public void reduce(List<Object> valueToReduce) {
      reducerHandle.reduce(valueToReduce);
    }

    /** Get reduced value */
    public List<WArrayList> getReducedValue(MasterGlobalCommUsage master) {
      return reducerHandle.getReducedValue(master);
    }

    /**
     * Broadcast reduced value from master
     */
    public void broadcastValue(BlockMasterApi master) {
      broadcastHandle = reducerHandle.broadcastValue(master);
    }

    /** Get broadcasted value */
    public List<WArrayList> getBroadcast(WorkerBroadcastUsage worker) {
      return broadcastHandle.getBroadcast(worker);
    }
  }
}
