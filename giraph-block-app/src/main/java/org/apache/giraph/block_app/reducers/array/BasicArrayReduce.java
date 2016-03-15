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

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi.CreateReducerFunctionApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.hadoop.io.Writable;

/**
 * Efficient generic primitive array reduce operation.
 *
 * Allows two modes - fixed size, and infinite size
 * (with keeping only actually used elements and resizing)
 *
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public class BasicArrayReduce<S, R extends Writable>
    implements ReduceOperation<Pair<IntRef, S>, WArrayList<R>> {
  private int fixedSize;
  private PrimitiveTypeOps<R> typeOps;
  private ReduceOperation<S, R> elementReduceOp;
  private R initialElement;
  private R reusable;
  private R reusable2;

  public BasicArrayReduce() {
  }


  /**
   * Create ReduceOperation that reduces BasicArrays by reducing individual
   * elements, with predefined size.
   *
   * @param fixedSize Number of elements
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   */
  public BasicArrayReduce(
      int fixedSize,
      PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp) {
    this.fixedSize = fixedSize;
    this.typeOps = typeOps;
    this.elementReduceOp = elementReduceOp;
    init();
  }


  /**
   * Create ReduceOperation that reduces BasicArrays by reducing individual
   * elements, with unbounded size.
   *
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   */
  public BasicArrayReduce(
      PrimitiveTypeOps<R> typeOps, ReduceOperation<S, R> elementReduceOp) {
    this(-1, typeOps, elementReduceOp);
  }


  /**
   * Registers one new local reducer, that will reduce BasicArray,
   * by reducing individual elements using {@code elementReduceOp},
   * with unbounded size.
   *
   * This function will return ReducerArrayHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param reduceApi API for creating reducers
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createLocalArrayHandles(
      PrimitiveTypeOps<R> typeOps, ReduceOperation<S, R> elementReduceOp,
      CreateReducersApi reduceApi) {
    return createLocalArrayHandles(-1, typeOps, elementReduceOp, reduceApi);
  }

  /**
   * Registers one new local reducer, that will reduce BasicArray,
   * by reducing individual elements using {@code elementReduceOp},
   * with predefined size.
   *
   * This function will return ReducerArrayHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param fixedSize Number of elements
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param reduceApi API for creating reducers
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createLocalArrayHandles(
      int fixedSize, PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp,
      final CreateReducersApi reduceApi) {
    return createArrayHandles(fixedSize, typeOps, elementReduceOp,
        new CreateReducerFunctionApi() {
          @Override
          public <S, R extends Writable> ReducerHandle<S, R> createReducer(
              ReduceOperation<S, R> reduceOp) {
            return reduceApi.createLocalReducer(reduceOp);
          }
        });
  }

  /**
   * Registers one new reducer, that will reduce BasicArray,
   * by reducing individual elements using {@code elementReduceOp},
   * with unbounded size.
   *
   * This function will return ReducerArrayHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param createFunction Function for creating a reducer
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createArrayHandles(
      PrimitiveTypeOps<R> typeOps, ReduceOperation<S, R> elementReduceOp,
      CreateReducerFunctionApi createFunction) {
    return createArrayHandles(-1, typeOps, elementReduceOp, createFunction);
  }

  /**
   * Registers one new reducer, that will reduce BasicArray,
   * by reducing individual elements using {@code elementReduceOp},
   * with predefined size.
   *
   * This function will return ReducerArrayHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param fixedSize Number of elements
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param createFunction Function for creating a reducer
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createArrayHandles(
      final int fixedSize, final PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp,
      CreateReducerFunctionApi createFunction) {
    final ReducerHandle<Pair<IntRef, S>, WArrayList<R>> reduceHandle =
        createFunction.createReducer(
            new BasicArrayReduce<>(fixedSize, typeOps, elementReduceOp));
    final IntRef curIndex = new IntRef(0);
    final R reusableValue = typeOps.create();
    final R initialValue = elementReduceOp.createInitialValue();
    final MutablePair<IntRef, S> reusablePair =
        MutablePair.of(new IntRef(0), null);
    final ReducerHandle<S, R> elementReduceHandle = new ReducerHandle<S, R>() {
      @Override
      public R getReducedValue(MasterGlobalCommUsage master) {
        WArrayList<R> result = reduceHandle.getReducedValue(master);
        if (fixedSize == -1 && curIndex.value >= result.size()) {
          typeOps.set(reusableValue, initialValue);
        } else {
          result.getIntoW(curIndex.value, reusableValue);
        }
        return reusableValue;
      }

      @Override
      public void reduce(S valueToReduce) {
        reusablePair.getLeft().value = curIndex.value;
        reusablePair.setRight(valueToReduce);
        reduceHandle.reduce(reusablePair);
      }

      @Override
      public BroadcastHandle<R> broadcastValue(BlockMasterApi master) {
        throw new UnsupportedOperationException();
      }
    };

    return new ReducerArrayHandle<S, R>() {
      @Override
      public ReducerHandle<S, R> get(int index) {
        curIndex.value = index;
        return elementReduceHandle;
      }

      @Override
      public int getStaticSize() {
        if (fixedSize == -1) {
          throw new UnsupportedOperationException(
              "Cannot call size, when one is not specified upfront");
        }
        return fixedSize;
      }

      @Override
      public int getReducedSize(BlockMasterApi master) {
        return reduceHandle.getReducedValue(master).size();
      }

      @Override
      public BroadcastArrayHandle<R> broadcastValue(BlockMasterApi master) {
        final BroadcastHandle<WArrayList<R>> broadcastHandle =
            reduceHandle.broadcastValue(master);
        final IntRef curIndex = new IntRef(0);
        final R reusableValue = typeOps.create();
        final BroadcastHandle<R>
        elementBroadcastHandle = new BroadcastHandle<R>() {
          @Override
          public R getBroadcast(WorkerBroadcastUsage worker) {
            WArrayList<R> result = broadcastHandle.getBroadcast(worker);
            if (fixedSize == -1 && curIndex.value >= result.size()) {
              typeOps.set(reusableValue, initialValue);
            } else {
              result.getIntoW(curIndex.value, reusableValue);
            }
            return reusableValue;
          }
        };
        return new BroadcastArrayHandle<R>() {
          @Override
          public BroadcastHandle<R> get(int index) {
            curIndex.value = index;
            return elementBroadcastHandle;
          }

          @Override
          public int getStaticSize() {
            if (fixedSize == -1) {
              throw new UnsupportedOperationException(
                  "Cannot call size, when one is not specified upfront");
            }
            return fixedSize;
          }

          @Override
          public int getBroadcastedSize(WorkerBroadcastUsage worker) {
            return broadcastHandle.getBroadcast(worker).size();
          }
        };
      }
    };
  }


  private void init() {
    initialElement = elementReduceOp.createInitialValue();
    reusable = typeOps.create();
    reusable2 = typeOps.create();
  }

  @Override
  public WArrayList<R> createInitialValue() {
    if (fixedSize != -1) {
      WArrayList<R> list = typeOps.createArrayList(fixedSize);
      fill(list, fixedSize);
      return list;
    } else {
      return typeOps.createArrayList(1);
    }
  }

  private void fill(WArrayList<R> list, int newSize) {
    if (fixedSize != -1 && newSize > fixedSize) {
      throw new IllegalArgumentException(newSize + " larger then " + fixedSize);
    }

    if (list.capacity() < newSize) {
      list.setCapacity(newSize);
    }
    while (list.size() < newSize) {
      list.addW(initialElement);
    }
  }

  @Override
  public WArrayList<R> reduce(
      WArrayList<R> curValue, Pair<IntRef, S> valueToReduce) {
    int index = valueToReduce.getLeft().value;
    fill(curValue, index + 1);
    curValue.getIntoW(index, reusable);
    R result = elementReduceOp.reduce(reusable, valueToReduce.getRight());
    curValue.setW(index, result);
    return curValue;
  }

  @Override
  public WArrayList<R> reduceMerge(
      WArrayList<R> curValue, WArrayList<R> valueToReduce) {
    fill(curValue, valueToReduce.size());
    for (int i = 0; i < valueToReduce.size(); i++) {
      valueToReduce.getIntoW(i, reusable2);
      curValue.getIntoW(i, reusable);
      R result = elementReduceOp.reduceMerge(reusable, reusable2);
      curValue.setW(i, result);
    }

    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(fixedSize);
    TypeOpsUtils.writeTypeOps(typeOps, out);
    WritableUtils.writeWritableObject(elementReduceOp, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fixedSize = in.readInt();
    typeOps = TypeOpsUtils.readTypeOps(in);
    elementReduceOp = WritableUtils.readWritableObject(in, null);
    init();
  }
}
