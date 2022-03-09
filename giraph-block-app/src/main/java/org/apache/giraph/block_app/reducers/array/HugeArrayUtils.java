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
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi.CreateReducerFunctionApi;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.ReducerArrayHandle;
import org.apache.giraph.block_app.reducers.array.ArrayOfHandles.ArrayOfBroadcasts;
import org.apache.giraph.block_app.reducers.array.ArrayOfHandles.ArrayOfReducers;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.function.ObjectHolder;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.giraph.worker.WorkerBroadcastUsage;
import org.apache.hadoop.io.Writable;

/**
 * Utility class when we are dealing with huge arrays (i.e. large number of
 * elements) within reducing/broadcasting.
 *
 * In Giraph, for each reducer there is a worker machine which is it's owner,
 * which does partial aggregation for it. So if we have only single huge
 * reducer - other workers will have to wait, while that single worker is doing
 * huge reducing operation. Additionally single reducer should be smaller then
 * max netty message, which is 1MB.
 * On the other hand, each reducer has a meaningful overhead, so we should try
 * to keep number of reducers as low as possible.
 *
 * By default we are being conservative, to keep individual reducers small,
 * with striping into 500k reducers by default. If you know exact sizes of
 * your objects you can specify exact number you want.
 *
 * So when we have huge array, we don't want one reducer/broadcast for each
 * element, but we also don't want one reducer/broadcast for the whole array.
 *
 * This class allows transparent split into reasonable number of reducers
 * (~500k), which solves both of the above issues.
 */
public class HugeArrayUtils {
  // Even with 100GB object, average stripe will be 200KB on average,
  // keeping outliers mostly under 1MB limit
  private static final IntConfOption NUM_STRIPES = new IntConfOption(
      "giraph.reducers.HugeArrayUtils.num_stripes", 500000,
      "Number of distict reducers to create. If array is smaller then this" +
      "number, each element will be it's own reducer");

  private HugeArrayUtils() { }

  /**
   * Create global array of reducers, by splitting the huge array
   * into NUM_STRIPES number of parts.
   *
   * @param fixedSize Number of elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param reduceApi Api for creating reducers
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createGlobalReducerArrayHandle(
      final int fixedSize, final ReduceOperation<S, R> elementReduceOp,
      final CreateReducersApi reduceApi) {
    return createGlobalReducerArrayHandle(
        fixedSize, elementReduceOp, reduceApi,
        NUM_STRIPES.get(reduceApi.getConf()));
  }

  /**
   * Create global array of reducers, by splitting the huge array
   * into {@code maxNumStripes} number of parts.
   *
   * @param fixedSize Number of elements
   * @param elementReduceOp ReduceOperation for individual elements
   * @param reduceApi Api for creating reducers
   * @param maxNumStripes Maximal number of reducers to create.
   * @return Created ReducerArrayHandle
   */
  public static <S, R extends Writable>
  ReducerArrayHandle<S, R> createGlobalReducerArrayHandle(
      final int fixedSize, final ReduceOperation<S, R> elementReduceOp,
      final CreateReducersApi reduceApi, int maxNumStripes) {
    PrimitiveTypeOps<R> typeOps = TypeOpsUtils.getPrimitiveTypeOpsOrNull(
        (Class<R>) elementReduceOp.createInitialValue().getClass());

    final CreateReducerFunctionApi
    createReducer = new CreateReducerFunctionApi() {
      @Override
      public <S, R extends Writable> ReducerHandle<S, R> createReducer(
          ReduceOperation<S, R> reduceOp) {
        return reduceApi.createGlobalReducer(reduceOp);
      }
    };

    if (fixedSize < maxNumStripes) {
      return new ArrayOfReducers<>(
          fixedSize,
          new Supplier<ReducerHandle<S, R>>() {
            @Override
            public ReducerHandle<S, R> get() {
              return createReducer.createReducer(elementReduceOp);
            }
          });
    } else {
      final ObjectStriping striping =
          new ObjectStriping(fixedSize, maxNumStripes);

      final ArrayList<ReducerArrayHandle<S, R>> handles =
          new ArrayList<>(striping.getSplits());
      for (int i = 0; i < striping.getSplits(); i++) {
        if (typeOps != null) {
          handles.add(BasicArrayReduce.createArrayHandles(
              striping.getSplitSize(i), typeOps,
              elementReduceOp, createReducer));
        } else {
          handles.add(ArrayReduce.createArrayHandles(
              striping.getSplitSize(i), elementReduceOp, createReducer));
        }
      }

      return new ReducerArrayHandle<S, R>() {
        @Override
        public ReducerHandle<S, R> get(int index) {
          if ((index >= fixedSize) || (index < 0)) {
            throw new RuntimeException(
                "Reducer Access out of bounds: requested : " +
                    index + " from array of size : " + fixedSize);
          }
          int reducerIndex = striping.getSplitIndex(index);
          int insideIndex = striping.getInsideIndex(index);
          return handles.get(reducerIndex).get(insideIndex);
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
        public BroadcastArrayHandle<R> broadcastValue(BlockMasterApi master) {
          throw new UnsupportedOperationException("for now not supported");
        }
      };
    }
  }

  /**
   * Broadcast a huge array, by splitting into NUM_STRIPES number of parts.
   *
   * @param count Number of elements
   * @param valueSupplier Supplier of value to be broadcasted for a given index
   * @param master Master API
   * @return Created BroadcastArrayHandle
   */
  public static <V extends Writable> BroadcastArrayHandle<V> broadcast(
      final int count,
      final Int2ObjFunction<V> valueSupplier,
      final BlockMasterApi master) {
    return broadcast(count, valueSupplier, null, master);
  }

  /**
   * Broadcast a huge array, by splitting into NUM_STRIPES number of parts.
   * Efficient for primitive types, using BasicArray underneath.
   *
   * @param count Number of elements
   * @param valueSupplier Supplier of value to be broadcasted for a given index
   * @param typeOps Element TypeOps
   * @param master Master API
   * @return Created BroadcastArrayHandle
   */
  public static <V extends Writable> BroadcastArrayHandle<V> broadcast(
      final int count,
      final Int2ObjFunction<V> valueSupplier,
      final PrimitiveTypeOps<V> typeOps,
      final BlockMasterApi master) {
    int numStripes = NUM_STRIPES.get(master.getConf());
    if (count < numStripes) {
      return new ArrayOfBroadcasts<>(
          count,
          new Int2ObjFunction<BroadcastHandle<V>>() {
            @Override
            public BroadcastHandle<V> apply(int i) {
              // We create a copy because the valueSupplier might return a
              // reusable obj. This function is NOT safe if typeOps is null
              // & valueSupplier returns reusable
              return master.broadcast(
                typeOps != null ?
                typeOps.createCopy(valueSupplier.apply(i)) :
                valueSupplier.apply(i));
            }
          });
    } else {
      ObjectStriping striping = new ObjectStriping(count, numStripes);
      final Int2ObjFunction<BroadcastHandle<V>> handleSupplier;

      if (typeOps != null) {
        handleSupplier = getPrimitiveBroadcastHandleSupplier(
            valueSupplier, typeOps, master, striping);
      } else {
        handleSupplier = getObjectBroadcastHandleSupplier(
            valueSupplier, master, striping);
      }
      return new BroadcastArrayHandle<V>() {
        @Override
        public BroadcastHandle<V> get(int index) {
          if (index >= count || index < 0) {
            throw new RuntimeException(
                "Broadcast Access out of bounds: requested: " +
                  index + " from array of size : " + count);
          }
          return handleSupplier.apply(index);
        }

        @Override
        public int getBroadcastedSize(WorkerBroadcastUsage worker) {
          return count;
        }

        @Override
        public int getStaticSize() {
          return count;
        }
      };
    }
  }

  private static <V extends Writable>
  Int2ObjFunction<BroadcastHandle<V>> getObjectBroadcastHandleSupplier(
      final Int2ObjFunction<V> valueSupplier,
      final BlockMasterApi master, final ObjectStriping striping) {
    final ObjectHolder<Class<V>> elementClass = new ObjectHolder<>();
    final ArrayOfHandles<BroadcastHandle<ArrayWritable<V>>> arrayOfBroadcasts =
      new ArrayOfHandles<>(
        striping.getSplits(),
        new Int2ObjFunction<BroadcastHandle<ArrayWritable<V>>>() {
          @Override
          public BroadcastHandle<ArrayWritable<V>> apply(int value) {
            int size = striping.getSplitSize(value);
            int start = striping.getSplitStart(value);
            V[] array = (V[]) new Writable[size];
            for (int i = 0; i < size; i++) {
              array[i] = valueSupplier.apply(start + i);
              if (elementClass.get() == null) {
                elementClass.apply((Class<V>) array[i].getClass());
              }
            }
            return master.broadcast(
                new ArrayWritable<>(elementClass.get(), array));
          }
        });

    final IntRef insideIndex = new IntRef(-1);
    final ObjectHolder<BroadcastHandle<ArrayWritable<V>>> handleHolder =
        new ObjectHolder<>();

    final BroadcastHandle<V> reusableHandle = new BroadcastHandle<V>() {
      @Override
      public V getBroadcast(WorkerBroadcastUsage worker) {
        return handleHolder.get().getBroadcast(worker).get()[insideIndex.value];
      }
    };

    return createBroadcastHandleSupplier(
        striping, arrayOfBroadcasts, insideIndex, handleHolder,
        reusableHandle);
  }

  private static <V extends Writable>
  Int2ObjFunction<BroadcastHandle<V>> getPrimitiveBroadcastHandleSupplier(
      final Int2ObjFunction<V> valueSupplier, final PrimitiveTypeOps<V> typeOps,
      final BlockMasterApi master, final ObjectStriping striping) {
    final ArrayOfHandles<BroadcastHandle<WArrayList<V>>> arrayOfBroadcasts =
      new ArrayOfHandles<>(
        striping.getSplits(),
        new Int2ObjFunction<BroadcastHandle<WArrayList<V>>>() {
          @Override
          public BroadcastHandle<WArrayList<V>> apply(int value) {
            int size = striping.getSplitSize(value);
            int start = striping.getSplitStart(value);
            WArrayList<V> array = typeOps.createArrayList(size);
            for (int i = 0; i < size; i++) {
              array.addW(valueSupplier.apply(start + i));
            }
            return master.broadcast(array);
          }
        });

    final IntRef insideIndex = new IntRef(-1);
    final ObjectHolder<BroadcastHandle<WArrayList<V>>> handleHolder =
            new ObjectHolder<>();
    final BroadcastHandle<V> reusableHandle = new BroadcastHandle<V>() {
      private final V reusable = typeOps.create();
      @Override
      public V getBroadcast(WorkerBroadcastUsage worker) {
        handleHolder.get().getBroadcast(worker).getIntoW(
            insideIndex.value, reusable);
        return reusable;
      }
    };

    return createBroadcastHandleSupplier(
        striping, arrayOfBroadcasts, insideIndex, handleHolder,
        reusableHandle);
  }

  private static <V extends Writable, A>
  Int2ObjFunction<BroadcastHandle<V>> createBroadcastHandleSupplier(
      final ObjectStriping striping,
      final ArrayOfHandles<BroadcastHandle<A>> arrayOfBroadcasts,
      final IntRef insideIndex,
      final ObjectHolder<BroadcastHandle<A>> handleHolder,
      final BroadcastHandle<V> reusableHandle) {
    final Int2ObjFunction<BroadcastHandle<V>> handleProvider =
        new Int2ObjFunction<BroadcastHandle<V>>() {
      @Override
      public BroadcastHandle<V> apply(int index) {
        int broadcastIndex = striping.getSplitIndex(index);
        insideIndex.value = striping.getInsideIndex(index);
        handleHolder.apply(arrayOfBroadcasts.get(broadcastIndex));
        return reusableHandle;
      }
    };
    return handleProvider;
  }

  /**
   * Handles indices calculations when spliting one range into smaller number
   * of splits, where indices stay consecutive.
   */
  static class ObjectStriping {
    private final int splits;
    private final int indicesPerObject;
    private final int overflowNum;
    private final int beforeOverflow;

    public ObjectStriping(int size, int splits) {
      this.splits = splits;
      this.indicesPerObject = size / splits;
      this.overflowNum = size % splits;
      this.beforeOverflow = overflowNum * (indicesPerObject + 1);
    }

    public int getSplits() {
      return splits;
    }

    public int getSplitSize(int splitIndex) {
      return indicesPerObject + (splitIndex < overflowNum ? 1 : 0);
    }

    public int getSplitStart(int splitIndex) {
      if (splitIndex < overflowNum) {
        return splitIndex * (indicesPerObject + 1);
      } else {
        return beforeOverflow + (splitIndex - overflowNum) * indicesPerObject;
      }
    }

    public int getSplitIndex(int objectIndex) {
      if (objectIndex < beforeOverflow) {
        return objectIndex / (indicesPerObject + 1);
      } else {
        return (objectIndex - beforeOverflow) / indicesPerObject + overflowNum;
      }
    }

    public int getInsideIndex(int objectIndex) {
      if (objectIndex < beforeOverflow) {
        return objectIndex % (indicesPerObject + 1);
      } else {
        return (objectIndex - beforeOverflow) % indicesPerObject;
      }
    }
  }
}
