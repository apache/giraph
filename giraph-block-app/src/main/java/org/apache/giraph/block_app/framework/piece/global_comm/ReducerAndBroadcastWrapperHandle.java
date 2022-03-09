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
package org.apache.giraph.block_app.framework.piece.global_comm;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.worker.WorkerBroadcastUsage;

/**
 * Handle that wraps both reducerHandle and broadcastHandle, so callers
 * don't need to have two fields.
 *
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public class ReducerAndBroadcastWrapperHandle<S, R> {
  private ReducerHandle<S, R> reducerHandle;
  private BroadcastHandle<R> broadcastHandle;

  /** Set reducer handle to just registered handle */
  public void registeredReducer(ReducerHandle<S, R> reducerHandle) {
    this.reducerHandle = reducerHandle;
  }

  /** Reduce single value */
  public void reduce(S valueToReduce) {
    reducerHandle.reduce(valueToReduce);
  }

  /** Get reduced value */
  public R getReducedValue(MasterGlobalCommUsage master) {
    return reducerHandle.getReducedValue(master);
  }

  /**
   * Broadcast reduced value from master
   */
  public void broadcastValue(BlockMasterApi master) {
    broadcastHandle = reducerHandle.broadcastValue(master);
  }

  /** Get broadcasted value */
  public R getBroadcast(WorkerBroadcastUsage worker) {
    return broadcastHandle.getBroadcast(worker);
  }
}
