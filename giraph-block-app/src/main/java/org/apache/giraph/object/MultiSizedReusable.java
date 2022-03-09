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
package org.apache.giraph.object;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicSet;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Holds reusable objects of multiple sizes.
 * Example usecase, is when we need a hashmap - that we will insert and iterate
 * on, both clear() and iterate method depend on size. And if we want to reuse
 * objects, we want to have multiple objects of different sizes, that we will
 * reuse.
 *
 * Instead of creating object for each distinct size, it creates objects with
 * first larger power of 2.
 *
 * @param <T> Type of reusable object
 */
public class MultiSizedReusable<T> implements Int2ObjFunction<T> {
  private final Int2ObjFunction<T> createSized;
  private final Consumer<T> init;
  @SuppressWarnings("unchecked")
  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private final transient T[] holder = (T[]) new Object[Integer.SIZE];

  // No-arg constructor Kryo can call to initialize holder
  MultiSizedReusable() {
    this(null, null);
  }

  public MultiSizedReusable(Int2ObjFunction<T> createSized, Consumer<T> init) {
    this.createSized = createSized;
    this.init = init;
  }

  @Override
  public T apply(int size) {
    Preconditions.checkArgument(size >= 0);
    int shiftBits = (Integer.SIZE -
        Integer.numberOfLeadingZeros(Math.max(0, size - 1))) / 2;
    T result = holder[shiftBits];
    if (result == null) {
      if (shiftBits >= 15) {
        result = createSized.apply(Integer.MAX_VALUE);
      } else {
        result = createSized.apply(1 << (shiftBits * 2 + 1));
      }
      holder[shiftBits] = result;
    }
    if (init != null) {
      init.apply(result);
    }
    return result;
  }

  public static <I> MultiSizedReusable<BasicSet<I>> createForBasicSet(
      final PrimitiveIdTypeOps<I> idTypeOps) {
    return new MultiSizedReusable<>(
        new Int2ObjFunction<BasicSet<I>>() {
          @Override
          public BasicSet<I> apply(int value) {
            return idTypeOps.createOpenHashSet(value);
          }
        },
        new Consumer<BasicSet<I>>() {
          @Override
          public void apply(BasicSet<I> t) {
            t.clear();
          }
        });
  }

  public static <K, V>
  MultiSizedReusable<Basic2ObjectMap<K, V>> createForBasic2ObjectMap(
      final PrimitiveIdTypeOps<K> idTypeOps) {
    return new MultiSizedReusable<>(
        new Int2ObjFunction<Basic2ObjectMap<K, V>>() {
          @Override
          public Basic2ObjectMap<K, V> apply(int value) {
            return idTypeOps.create2ObjectOpenHashMap(value, null);
          }
        },
        new Consumer<Basic2ObjectMap<K, V>>() {
          @Override
          public void apply(Basic2ObjectMap<K, V> t) {
            t.clear();
          }
        });
  }
}
