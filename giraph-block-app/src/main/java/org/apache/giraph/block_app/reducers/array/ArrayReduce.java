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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi.CreateReducerFunctionApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.hadoop.io.Writable;

/**
 * One reducer representing reduction of array of individual values.
 * Elements are represented as object, and so BasicArrayReduce should be
 * used instead when elements are primitive types.
 *
 * @param <S> Single value type, objects passed on workers
 * @param <R> Reduced value type
 */
public class ArrayReduce<S, R extends Writable>
    implements ReduceOperation<Pair<IntRef, S>, ArrayWritable<R>> {
  private int fixedSize;
  private ReduceOperation<S, R> elementReduceOp;
  private Class<R> elementClass;

  public ArrayReduce() {
  }

  /**
   * Create ReduceOperation that reduces arrays by reducing individual
   * elements.
   *
   * @param fixedSize Number of elements
   * @param elementReduceOp ReduceOperation for individual elements
   */
  public ArrayReduce(int fixedSize, ReduceOperation<S, R> elementReduceOp) {
    this.fixedSize = fixedSize;
    this.elementReduceOp = elementReduceOp;
    init();
  }

  /**
   * Registers one new reducer, that will reduce array of objects,
   * by reducing individual elements using {@code elementReduceOp}.
   *
   * This function will return ReducerArrayHandle to it, by which
   * individual elements can be manipulated separately.
   *
   * @param fixedSize Number of elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param createFunction Function for creating a reducer
   * @return Created ReducerArrayHandle
   */
  public static <S, T extends Writable>
  ReducerArrayHandle<S, T> createArrayHandles(
      final int fixedSize, ReduceOperation<S, T> elementReduceOp,
      CreateReducerFunctionApi createFunction) {
    final ReducerHandle<Pair<IntRef, S>, ArrayWritable<T>> reduceHandle =
        createFunction.createReducer(
            new ArrayReduce<>(fixedSize, elementReduceOp));

    final IntRef curIndex = new IntRef(0);
    final MutablePair<IntRef, S> reusablePair =
        MutablePair.of(new IntRef(0), null);
    final ReducerHandle<S, T> elementReduceHandle = new ReducerHandle<S, T>() {
      @Override
      public T getReducedValue(MasterGlobalCommUsage master) {
        ArrayWritable<T> result = reduceHandle.getReducedValue(master);
        return result.get()[curIndex.value];
      }

      @Override
      public void reduce(S valueToReduce) {
        reusablePair.getLeft().value = curIndex.value;
        reusablePair.setRight(valueToReduce);
        reduceHandle.reduce(reusablePair);
      }

      @Override
      public BroadcastHandle<T> broadcastValue(BlockMasterApi master) {
        throw new UnsupportedOperationException();
      }
    };

    return new ReducerArrayHandle<S, T>() {
      @Override
      public ReducerHandle<S, T> get(int index) {
        curIndex.value = index;
        return elementReduceHandle;
      }

      @Override
      public int getStaticSize() {
        return fixedSize;
      }

      @Override
      public int getReducedSize(BlockMasterApi master) {
        return getStaticSize();
      }

      @Override
      public BroadcastArrayHandle<T> broadcastValue(BlockMasterApi master) {
        final BroadcastHandle<ArrayWritable<T>> broadcastHandle =
            reduceHandle.broadcastValue(master);
        final IntRef curIndex = new IntRef(0);
        final BroadcastHandle<T>
        elementBroadcastHandle = new BroadcastHandle<T>() {
          @Override
          public T getBroadcast(WorkerBroadcastUsage worker) {
            ArrayWritable<T> result = broadcastHandle.getBroadcast(worker);
            return result.get()[curIndex.value];
          }
        };
        return new BroadcastArrayHandle<T>() {
          @Override
          public BroadcastHandle<T> get(int index) {
            curIndex.value = index;
            return elementBroadcastHandle;
          }

          @Override
          public int getStaticSize() {
            return fixedSize;
          }

          @Override
          public int getBroadcastedSize(WorkerBroadcastUsage worker) {
            return getStaticSize();
          }
        };
      }
    };
  }

  private void init() {
    elementClass = (Class<R>) elementReduceOp.createInitialValue().getClass();
  }

  @Override
  public ArrayWritable<R> createInitialValue() {
    R[] values = (R[]) Array.newInstance(elementClass, fixedSize);
    for (int i = 0; i < fixedSize; i++) {
      values[i] = elementReduceOp.createInitialValue();
    }
    return new ArrayWritable<>(elementClass, values);
  }

  @Override
  public ArrayWritable<R> reduce(
      ArrayWritable<R> curValue, Pair<IntRef, S> valueToReduce) {
    int index = valueToReduce.getLeft().value;
    curValue.get()[index] =
        elementReduceOp.reduce(curValue.get()[index], valueToReduce.getRight());
    return curValue;
  }

  @Override
  public ArrayWritable<R> reduceMerge(
      ArrayWritable<R> curValue, ArrayWritable<R> valueToReduce) {
    for (int i = 0; i < fixedSize; i++) {
      curValue.get()[i] =
          elementReduceOp.reduceMerge(
              curValue.get()[i], valueToReduce.get()[i]);
    }
    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(fixedSize);
    WritableUtils.writeWritableObject(elementReduceOp, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fixedSize = in.readInt();
    elementReduceOp = WritableUtils.readWritableObject(in, null);
    init();
  }

}
