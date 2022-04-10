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

import org.apache.giraph.types.ops.ShortTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.ReusableIterator;
import org.apache.hadoop.io.ShortWritable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * ShortWritable implementation of Basic2ObjectMap.
 *
 * @param <V> Value type
 */
public final class BasicShort2ObjectOpenHashMap<V>
    extends Basic2ObjectMap<ShortWritable, V> {
  /** Map */
  private final Short2ObjectOpenHashMap<V> map;
  /** Value writer */
  private final WritableWriter<V> valueWriter;

  /**
   * Constructor
   *
   * @param valueWriter Writer of values
   */
  public BasicShort2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
    this.map = new Short2ObjectOpenHashMap<>();
    this.valueWriter = valueWriter;
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   * @param valueWriter Writer of values
   */
  public BasicShort2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    this.map = new Short2ObjectOpenHashMap<>(capacity);
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
  public boolean containsKey(ShortWritable key) {
    return map.containsKey(key.get());
  }

  @Override
  public V put(ShortWritable key, V value) {
    return map.put(key.get(), value);
  }

  @Override
  public V get(ShortWritable key) {
    return map.get(key.get());
  }

  @Override
  public V remove(ShortWritable key) {
    return map.remove(key.get());
  }

  @Override
  public PrimitiveIdTypeOps<ShortWritable> getKeyTypeOps() {
    return ShortTypeOps.INSTANCE;
  }

  @Override
  public Iterator<ShortWritable> fastKeyIterator() {
    return new ReusableIterator<ShortIterator>(map.keySet().iterator()) {
      @Override
      public ShortWritable next() {
        reusableKey.set(iter.nextShort());
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
    ObjectIterator<Short2ObjectMap.Entry<V>> iterator =
        map.short2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Short2ObjectMap.Entry<V> entry = iterator.next();
      out.writeShort(entry.getShortKey());
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
      short key = in.readShort();
      V value = valueWriter.readFields(in);
      map.put(key, value);
    }
  }
}
