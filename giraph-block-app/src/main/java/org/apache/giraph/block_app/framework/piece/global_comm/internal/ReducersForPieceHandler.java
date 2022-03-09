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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.giraph.worker.WorkerReduceUsage;
import org.apache.hadoop.io.Writable;

/**
 * All logic for transforming Giraph's reducer API to reducer handles.
 * Contains state of active reducers, and is kept within a Piece.
 */
public class ReducersForPieceHandler implements VertexSenderObserver {
  private static final AtomicInteger HANDLER_COUNTER = new AtomicInteger();
  private static final AtomicInteger BROADCAST_COUNTER = new AtomicInteger();

  private final int handleIndex = HANDLER_COUNTER.incrementAndGet();
  private final AtomicInteger reduceCounter = new AtomicInteger();

  private final ArrayList<VertexSenderObserver> observers = new ArrayList<>();

  @Override
  public void vertexSenderWorkerPreprocess(WorkerReduceUsage usage) {
    for (VertexSenderObserver observer : observers) {
      observer.vertexSenderWorkerPreprocess(usage);
    }
  }

  @Override
  public void vertexSenderWorkerPostprocess(WorkerReduceUsage usage) {
    for (VertexSenderObserver observer : observers) {
      observer.vertexSenderWorkerPostprocess(usage);
    }
  }

  public <S, R extends Writable> ReducerHandle<S, R> createLocalReducer(
      MasterGlobalCommUsage master,  ReduceOperation<S, R> reduceOp,
      R globalInitialValue) {
    LocalReduceHandle<S, R> handle = new LocalReduceHandle<>(reduceOp);
    master.registerReducer(handle.getName(), reduceOp, globalInitialValue);
    observers.add(handle);
    return handle;
  }

  public <S, R extends Writable> ReducerHandle<S, R> createGlobalReducer(
      MasterGlobalCommUsage master,  ReduceOperation<S, R> reduceOp,
      R globalInitialValue) {
    ReduceHandleImpl<S, R> handle = new GlobalReduceHandle<>(reduceOp);
    master.registerReducer(handle.getName(), reduceOp, globalInitialValue);
    observers.add(handle);
    return handle;
  }

  /**
   * Implementation of BroadcastHandle
   *
   * @param <T> Value type
   */
  public static class BroadcastHandleImpl<T> implements BroadcastHandle<T> {
    private final String name;

    public BroadcastHandleImpl() {
      this.name = "_utils.broadcast." + BROADCAST_COUNTER.incrementAndGet();
    }

    public String getName() {
      return name;
    }

    @Override
    public T getBroadcast(WorkerBroadcastUsage worker) {
      return worker.getBroadcast(name);
    }
  }

  /**
   * Parent implementation of ReducerHandle
   *
   * @param <S> Single value type
   * @param <R> Reduced value type
   */
  public abstract class ReduceHandleImpl<S, R extends Writable>
      implements ReducerHandle<S, R>, VertexSenderObserver {
    protected final ReduceOperation<S, R> reduceOp;
    private final String name;

    private ReduceHandleImpl(ReduceOperation<S, R> reduceOp) {
      this.reduceOp = reduceOp;
      name = "_utils." + handleIndex +
          ".reduce." + reduceCounter.incrementAndGet();
    }

    public String getName() {
      return name;
    }

    @Override
    public R getReducedValue(MasterGlobalCommUsage master) {
      return master.getReduced(name);
    }

    @Override
    public BroadcastHandle<R> broadcastValue(BlockMasterApi master) {
      return unwrapHandle(master.broadcast(
          new WrappedReducedValue<>(reduceOp, getReducedValue(master))));
    }
  }

  private static <R extends Writable> BroadcastHandle<R> unwrapHandle(
      final BroadcastHandle<WrappedReducedValue<R>> handle) {
    return new BroadcastHandle<R>() {
      @Override
      public R getBroadcast(WorkerBroadcastUsage worker) {
        return handle.getBroadcast(worker).getValue();
      }
    };
  }

  /**
   * Wrapper that makes reduced values self-serializable,
   * and allows them to be broadcasted.
   *
   * @param <R> Reduced value type
   */
  public static class WrappedReducedValue<R extends Writable>
      implements Writable {
    private ReduceOperation<?, R> reduceOp;
    private R value;

    public WrappedReducedValue() {
    }

    public WrappedReducedValue(ReduceOperation<?, R> reduceOp, R value) {
      this.reduceOp = reduceOp;
      this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeWritableObject(reduceOp, out);
      value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      reduceOp = WritableUtils.readWritableObject(in, null);
      value = reduceOp.createInitialValue();
      value.readFields(in);
    }

    public R getValue() {
      return value;
    }
  }

  /**
   * Global Reduce Handle is implementation of ReducerHandle, that will keep
   * only one value for each worker, and each call to reduce will have
   * to obtain a global lock, and incur synchronization costs.
   * Use only when objects are so large, that having many copies cannot fit
   * into memory.
   *
   * @param <S> Single value type
   * @param <R> Reduced value type
   */
  public class GlobalReduceHandle<S, R extends Writable>
      extends ReduceHandleImpl<S, R> {
    private transient WorkerReduceUsage usage;

    public GlobalReduceHandle(ReduceOperation<S, R> reduceOp) {
      super(reduceOp);
    }

    @Override
    public void vertexSenderWorkerPreprocess(WorkerReduceUsage usage) {
      this.usage = usage;
    }

    @Override
    public void reduce(S valueToReduce) {
      usage.reduce(getName(), valueToReduce);
    }

    @Override
    public void vertexSenderWorkerPostprocess(WorkerReduceUsage usage) {
    }
  }

  /**
   * Local Reduce Handle is implementation of ReducerHandle, that will make a
   * partially reduced value on each worker thread, which are at the end
   * reduced all together.
   * This is preferred implementation, unless it cannot be used due to memory
   * overhead, because all partially reduced values will not fit the memory.
   *
   * @param <S> Single value type
   * @param <R> Reduced value type
   */
  public class LocalReduceHandle<S, R extends Writable>
      extends ReduceHandleImpl<S, R> {
    private transient Reducer<S, R> reducer;

    public LocalReduceHandle(ReduceOperation<S, R> reduceOp) {
      super(reduceOp);
    }

    @Override
    public void vertexSenderWorkerPreprocess(WorkerReduceUsage usage) {
      this.reducer = new Reducer<>(reduceOp);
    }

    @Override
    public void reduce(S valueToReduce) {
      reducer.reduce(valueToReduce);
    }

    @Override
    public void vertexSenderWorkerPostprocess(WorkerReduceUsage usage) {
      usage.reduceMerge(getName(), reducer.getCurrentValue());
    }
  }
}
