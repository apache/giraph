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
package org.apache.giraph.block_app.framework.api;

import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/**
 * Api for creating reducer handles.
 */
public interface CreateReducersApi extends BlockConfApi {

  /**
   * Create local reducer, returning a handle to it.
   *
   * Local reducer means that each worker thread has it's own local partially
   * reduced value, which are at the end reduced all together.
   * Preferable, unless it cannot be used, because all copies of the object
   * do not fit the memory.
   */
  <S, R extends Writable> ReducerHandle<S, R> createLocalReducer(
      ReduceOperation<S, R> reduceOp);

  /**
   * Create local reducer, returning a handle to it.
   *
   * Local reducer means that each worker thread has it's own local partially
   *  reduced value, which are at the end reduced all together.
   * Preferable, unless it cannot be used, because all copies of the object
   * do not fit the memory.
   */
  <S, R extends Writable> ReducerHandle<S, R> createLocalReducer(
      ReduceOperation<S, R> reduceOp, R globalInitialValue);

  /**
   * Create global reducer, returning a handle to it.
   *
   * Global reducer means that there is only one value for each worker,
   * and each call to reduce will have to obtain a global lock, and incur
   * synchronization costs.
   * Use only when objects are so large, that having many copies cannot
   * fit into memory.
   */
  <S, R extends Writable> ReducerHandle<S, R> createGlobalReducer(
      ReduceOperation<S, R> reduceOp);

  /**
   * Create global reducer, returning a handle to it.
   *
   * Global reducer means that there is only one value for each worker,
   * and each call to reduce will have to obtain a global lock, and incur
   * synchronization costs.
   * Use only when objects are so large, that having many copies cannot
   * fit into memory.
   */
  <S, R extends Writable> ReducerHandle<S, R> createGlobalReducer(
      ReduceOperation<S, R> reduceOp, R globalInitialValue);

  /**
   * Function that creates a reducer - abstracting away whether it is
   * local or global reducer
   */
  public interface CreateReducerFunctionApi {
    <S, R extends Writable> ReducerHandle<S, R> createReducer(
        ReduceOperation<S, R> reduceOp);
  }
}
