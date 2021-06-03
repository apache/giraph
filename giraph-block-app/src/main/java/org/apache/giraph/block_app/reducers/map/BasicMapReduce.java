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
package org.apache.giraph.block_app.reducers.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi.CreateReducerFunctionApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.map.BroadcastMapHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.map.ReducerMapHandle;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Efficient generic primitive map of values reduce operation.
 * (it is BasicMap Reduce, not to be confused with MapReduce)
 *
 * @param <K> Key type
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public class BasicMapReduce<K extends WritableComparable, S,
    R extends Writable>
    implements ReduceOperation<Pair<K, S>, Basic2ObjectMap<K, R>> {
  private PrimitiveIdTypeOps<K> keyTypeOps;
  private PrimitiveTypeOps<R> typeOps;
  private ReduceOperation<S, R> elementReduceOp;
  private WritableWriter<R> writer;

  public BasicMapReduce() {
  }

  /**
   * Create ReduceOperation that reduces BasicMaps by reducing individual
   * elements corresponding to the same key.
   *
   * @param keyTypeOps TypeOps of keys
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   */
  public BasicMapReduce(
      PrimitiveIdTypeOps<K> keyTypeOps, PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp) {
    this.keyTypeOps = keyTypeOps;
    this.typeOps = typeOps;
    this.elementReduceOp = elementReduceOp;
    init();
  }

  /**
   * Registers one new local reducer, that will reduce BasicMap,
   * by reducing individual elements corresponding to the same key
   * using {@code elementReduceOp}.
   *
   * This function will return ReducerMapHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param keyTypeOps TypeOps of keys
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param reduceApi API for creating reducers
   * @return Created ReducerMapHandle
   */
  public static <K extends WritableComparable, S, R extends Writable>
  ReducerMapHandle<K, S, R> createLocalMapHandles(
      PrimitiveIdTypeOps<K> keyTypeOps, PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp,
      final CreateReducersApi reduceApi) {
    return createMapHandles(
        keyTypeOps, typeOps, elementReduceOp,
        new CreateReducerFunctionApi() {
          @Override
          public <S, R extends Writable> ReducerHandle<S, R> createReducer(
              ReduceOperation<S, R> reduceOp) {
            return reduceApi.createLocalReducer(reduceOp);
          }
        });
  }

  /**
   * Registers one new reducer, that will reduce BasicMap,
   * by reducing individual elements corresponding to the same key
   * using {@code elementReduceOp}.
   *
   * This function will return ReducerMapHandle, by which
   * individual elements can be manipulated separately.
   *
   * @param keyTypeOps TypeOps of keys
   * @param typeOps TypeOps of individual elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param createFunction Function for creating a reducer
   * @return Created ReducerMapHandle
   */
  public static <K extends WritableComparable, S, R extends Writable>
  ReducerMapHandle<K, S, R> createMapHandles(
      final PrimitiveIdTypeOps<K> keyTypeOps, final PrimitiveTypeOps<R> typeOps,
      ReduceOperation<S, R> elementReduceOp,
      CreateReducerFunctionApi createFunction) {
    final ReducerHandle<Pair<K, S>, Basic2ObjectMap<K, R>> reduceHandle =
      createFunction.createReducer(
          new BasicMapReduce<>(keyTypeOps, typeOps, elementReduceOp));
    final K curIndex = keyTypeOps.create();
    final R reusableValue = typeOps.create();
    final R initialValue = elementReduceOp.createInitialValue();
    final MutablePair<K, S> reusablePair = MutablePair.of(null, null);
    final ReducerHandle<S, R> elementReduceHandle = new ReducerHandle<S, R>() {
      @Override
      public R getReducedValue(MasterGlobalCommUsage master) {
        Basic2ObjectMap<K, R> result = reduceHandle.getReducedValue(master);
        R value = result.get(curIndex);
        if (value == null) {
          typeOps.set(reusableValue, initialValue);
        } else {
          typeOps.set(reusableValue, value);
        }
        return reusableValue;
      }

      @Override
      public void reduce(S valueToReduce) {
        reusablePair.setLeft(curIndex);
        reusablePair.setRight(valueToReduce);
        reduceHandle.reduce(reusablePair);
      }

      @Override
      public BroadcastHandle<R> broadcastValue(BlockMasterApi master) {
        throw new UnsupportedOperationException();
      }
    };

    return new ReducerMapHandle<K, S, R>() {
      @Override
      public ReducerHandle<S, R> get(K key) {
        keyTypeOps.set(curIndex, key);
        return elementReduceHandle;
      }

      @Override
      public int getReducedSize(BlockMasterApi master) {
        return reduceHandle.getReducedValue(master).size();
      }

      @Override
      public BroadcastMapHandle<K, R> broadcastValue(BlockMasterApi master) {
        final BroadcastHandle<Basic2ObjectMap<K, R>> broadcastHandle =
          reduceHandle.broadcastValue(master);
        final K curIndex = keyTypeOps.create();
        final R reusableValue = typeOps.create();
        final BroadcastHandle<R>
        elementBroadcastHandle = new BroadcastHandle<R>() {
          @Override
          public R getBroadcast(WorkerBroadcastUsage worker) {
            Basic2ObjectMap<K, R> result = broadcastHandle.getBroadcast(worker);
            R value = result.get(curIndex);
            if (value == null) {
              typeOps.set(reusableValue, initialValue);
            } else {
              typeOps.set(reusableValue, value);
            }
            return reusableValue;
          }
        };
        return new BroadcastMapHandle<K, R>() {
          @Override
          public BroadcastHandle<R> get(K key) {
            keyTypeOps.set(curIndex, key);
            return elementBroadcastHandle;
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
    writer = new WritableWriter<R>() {
      @Override
      public void write(DataOutput out, R value) throws IOException {
        value.write(out);
      }

      @Override
      public R readFields(DataInput in) throws IOException {
        R result = typeOps.create();
        result.readFields(in);
        return result;
      }
    };
  }

  @Override
  public Basic2ObjectMap<K, R> createInitialValue() {
    return keyTypeOps.create2ObjectOpenHashMap(writer);
  }

  @Override
  public Basic2ObjectMap<K, R> reduce(
      Basic2ObjectMap<K, R> curValue, Pair<K, S> valueToReduce) {
    R result = curValue.get(valueToReduce.getLeft());
    if (result == null) {
      result = typeOps.create();
    }
    result = elementReduceOp.reduce(result, valueToReduce.getRight());
    curValue.put(valueToReduce.getLeft(), result);
    return curValue;
  }

  @Override
  public Basic2ObjectMap<K, R> reduceMerge(
      Basic2ObjectMap<K, R> curValue, Basic2ObjectMap<K, R> valueToReduce) {
    for (Iterator<K> iter = valueToReduce.fastKeyIterator(); iter.hasNext();) {
      K key = iter.next();

      R result = curValue.get(key);
      if (result == null) {
        result = typeOps.create();
      }
      result = elementReduceOp.reduceMerge(result, valueToReduce.get(key));
      curValue.put(key, result);
    }
    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TypeOpsUtils.writeTypeOps(keyTypeOps, out);
    TypeOpsUtils.writeTypeOps(typeOps, out);
    WritableUtils.writeWritableObject(elementReduceOp, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    keyTypeOps = TypeOpsUtils.readTypeOps(in);
    typeOps = TypeOpsUtils.readTypeOps(in);
    elementReduceOp = WritableUtils.readWritableObject(in, null);
    init();
  }
}
