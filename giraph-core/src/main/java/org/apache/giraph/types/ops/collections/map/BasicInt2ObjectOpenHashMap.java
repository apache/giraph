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
package org.apache.giraph.types.ops.collections.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.ReusableIterator;
import org.apache.hadoop.io.IntWritable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * IntWritable implementation of Basic2ObjectMap.
 *
 * @param <V> Value type
 */
public final class BasicInt2ObjectOpenHashMap<V>
    extends Basic2ObjectMap<IntWritable, V> {
  /** Map */
  private final Int2ObjectOpenHashMap<V> map;
  /** Value writer */
  private final WritableWriter<V> valueWriter;

  /**
   * Constructor
   *
   * @param valueWriter Writer of values
   */
  public BasicInt2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
    this.map = new Int2ObjectOpenHashMap<>();
    this.valueWriter = valueWriter;
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   * @param valueWriter Writer of values
   */
  public BasicInt2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    this.map = new Int2ObjectOpenHashMap<>(capacity);
    this.valueWriter = valueWriter;
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean containsKey(IntWritable key) {
    return map.containsKey(key.get());
  }

  @Override
  public V put(IntWritable key, V value) {
    return map.put(key.get(), value);
  }

  @Override
  public V get(IntWritable key) {
    return map.get(key.get());
  }

  @Override
  public V remove(IntWritable key) {
    return map.remove(key.get());
  }

  @Override
  public PrimitiveIdTypeOps<IntWritable> getKeyTypeOps() {
    return IntTypeOps.INSTANCE;
  }

  @Override
  public Iterator<IntWritable> fastKeyIterator() {
    return new ReusableIterator<IntIterator>(map.keySet().iterator()) {
      @Override
      public IntWritable next() {
        reusableKey.set(iter.nextInt());
        return reusableKey;
      }

      @Override
      public void reset() {
        iter = map.keySet().iterator();
      }
    };
  }

  @Override
  public Iterator<V> valueIterator() {
    return map.values().iterator();
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkState(
      valueWriter != null,
      "valueWriter is not provided"
    );

    out.writeInt(map.size());
    ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
        map.int2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<V> entry = iterator.next();
      out.writeInt(entry.getIntKey());
      valueWriter.write(out, entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Preconditions.checkState(
      valueWriter != null,
      "valueWriter is not provided"
    );

    int size = in.readInt();
    map.clear();
    map.trim(size);
    while (size-- > 0) {
      int key = in.readInt();
      V value = valueWriter.readFields(in);
      map.put(key, value);
    }
  }
}
