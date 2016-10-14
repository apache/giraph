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
package org.apache.giraph.block_app.reducers.array;

import java.util.ArrayList;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.worker.WorkerBroadcastUsage;

/**
 * ArrayHandle implemented as an array of individual handles.
 *
 * @param <H> Handle type
 */
public class ArrayOfHandles<H> implements ArrayHandle<H> {
  protected final ArrayList<H> handles;

  public ArrayOfHandles(int count, Supplier<H> reduceHandleFactory) {
    handles = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      handles.add(reduceHandleFactory.get());
    }
  }

  public ArrayOfHandles(int count, Int2ObjFunction<H> reduceHandleFactory) {
    handles = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      handles.add(reduceHandleFactory.apply(i));
    }
  }

  @Override
  public H get(int index) {
    return handles.get(index);
  }

  @Override
  public int getStaticSize() {
    return handles.size();
  }

  /**
   * ReducerArrayHandle implemented as an array of separate reducer handles.
   *
   * @param <S> Handle type
   * @param <R> Reduce value type
   */
  public static class ArrayOfReducers<S, R>
      extends ArrayOfHandles<ReducerHandle<S, R>>
      implements ReducerArrayHandle<S, R> {

    public ArrayOfReducers(
        int count, Supplier<ReducerHandle<S, R>> reduceHandleFactory) {
      super(count, reduceHandleFactory);
    }

    public ArrayOfReducers(
        int count, Int2ObjFunction<ReducerHandle<S, R>> reduceHandleFactory) {
      super(count, reduceHandleFactory);
    }

    @Override
    public int getReducedSize(BlockMasterApi master) {
      return getStaticSize();
    }

    @Override
    public BroadcastArrayHandle<R> broadcastValue(final BlockMasterApi master) {
      return new ArrayOfBroadcasts<>(
          getStaticSize(),
          new Int2ObjFunction<BroadcastHandle<R>>() {
            @Override
            public BroadcastHandle<R> apply(int index) {
              return get(index).broadcastValue(master);
            }
          });
    }
  }

  /**
   * BroadcastArrayHandle implemented as an array of separate broadcast handles.
   *
   * @param <T> Handle type
   */
  public static class ArrayOfBroadcasts<T>
      extends ArrayOfHandles<BroadcastHandle<T>>
      implements BroadcastArrayHandle<T> {

    public ArrayOfBroadcasts(
        int count,
        Int2ObjFunction<BroadcastHandle<T>> broadcastHandleFactory) {
      super(count, broadcastHandleFactory);
    }

    public ArrayOfBroadcasts(
        int count,
        Supplier<BroadcastHandle<T>> broadcastHandleFactory) {
      super(count, broadcastHandleFactory);
    }

    @Override
    public int getBroadcastedSize(WorkerBroadcastUsage worker) {
      return getStaticSize();
    }
  }
}
