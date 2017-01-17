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
package org.apache.giraph.types.ops;

import org.apache.giraph.types.ops.collections.array.WShortArrayList;
import org.apache.giraph.types.ops.collections.map.BasicShort2ObjectOpenHashMap;
import org.apache.giraph.types.ops.collections.set.BasicShortOpenHashSet;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.hadoop.io.ShortWritable;

import java.io.DataInput;
import java.io.IOException;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** TypeOps implementation for working with ShortWritable type */
public enum ShortTypeOps implements
    PrimitiveIdTypeOps<ShortWritable>, NumericTypeOps<ShortWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<ShortWritable> getTypeClass() {
    return ShortWritable.class;
  }

  @Override
  public ShortWritable create() {
    return new ShortWritable();
  }

  @Override
  public ShortWritable createCopy(ShortWritable from) {
    return new ShortWritable(from.get());
  }

  @Override
  public void set(ShortWritable to, ShortWritable from) {
    to.set(from.get());
  }

  @Override
  public WShortArrayList createArrayList() {
    return new WShortArrayList();
  }

  @Override
  public WShortArrayList createArrayList(int capacity) {
    return new WShortArrayList(capacity);
  }

  @Override
  public WShortArrayList readNewArrayList(DataInput in) throws IOException {
    return WShortArrayList.readNew(in);
  }

  @Override
  public BasicShortOpenHashSet createOpenHashSet() {
    return new BasicShortOpenHashSet();
  }

  @Override
  public BasicShortOpenHashSet createOpenHashSet(long capacity) {
    return new BasicShortOpenHashSet((int) capacity);
  }

  @Override
  public <V> BasicShort2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      WritableWriter<V> valueWriter) {
    return new BasicShort2ObjectOpenHashMap<>(valueWriter);
  }

  @Override
  public <V> BasicShort2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    return new BasicShort2ObjectOpenHashMap<>(capacity, valueWriter);
  }

  @Override
  public ShortWritable createZero() {
    return new ShortWritable((short) 0);
  }

  @Override
  public ShortWritable createOne() {
    return new ShortWritable((short) 1);
  }

  @Override
  public ShortWritable createMinNegativeValue() {
    return new ShortWritable(Short.MIN_VALUE);
  }

  @Override
  public ShortWritable createMaxPositiveValue() {
    return new ShortWritable(Short.MAX_VALUE);
  }

  @Override
  public void plusInto(ShortWritable value, ShortWritable increment) {
    value.set((short) (value.get() + increment.get()));
  }

  @Override
  public void multiplyInto(ShortWritable value, ShortWritable multiplier) {
    value.set((short) (value.get() * multiplier.get()));
  }

  @Override
  public void negate(ShortWritable value) {
    value.set((short) (-value.get()));
  }

  @Override
  public int compare(ShortWritable value1, ShortWritable value2) {
    return Short.compare(value1.get(), value2.get());
  }
}
