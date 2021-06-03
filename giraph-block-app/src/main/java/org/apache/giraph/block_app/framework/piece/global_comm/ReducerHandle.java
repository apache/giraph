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

/**
 * Handle to a reducer.
 *
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public interface ReducerHandle<S, R> {
  /** Reduce single value */
  void reduce(S valueToReduce);
  /** Get reduced value */
  R getReducedValue(MasterGlobalCommUsage master);

  /**
   * Broadcast reduced value from master
   *
   * @return Handle to the broadcasted value.
   */
  BroadcastHandle<R> broadcastValue(BlockMasterApi master);
}
