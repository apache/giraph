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

import org.apache.giraph.types.ops.ByteTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.ReusableIterator;
import org.apache.hadoop.io.ByteWritable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * ByteWritable implementation of Basic2ObjectMap.
 *
 * @param <V> Value type
 */
public final class BasicByte2ObjectOpenHashMap<V>
    extends Basic2ObjectMap<ByteWritable, V> {
  /** Map */
  private final Byte2ObjectOpenHashMap<V> map;
  /** Value writer */
  private final WritableWriter<V> valueWriter;

  /**
   * Constructor
   *
   * @param valueWriter Writer of values
   */
  public BasicByte2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
    this.map = new Byte2ObjectOpenHashMap<>();
    this.valueWriter = valueWriter;
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   * @param valueWriter Writer of values
   */
  public BasicByte2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    this.map = new Byte2ObjectOpenHashMap<>(capacity);
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
  public boolean containsKey(ByteWritable key) {
    return map.containsKey(key.get());
  }

  @Override
  public V put(ByteWritable key, V value) {
    return map.put(key.get(), value);
  }

  @Override
  public V get(ByteWritable key) {
    return map.get(key.get());
  }

  @Override
  public V remove(ByteWritable key) {
    return map.remove(key.get());
  }

  @Override
  public PrimitiveIdTypeOps<ByteWritable> getKeyTypeOps() {
    return ByteTypeOps.INSTANCE;
  }

  @Override
  public Iterator<ByteWritable> fastKeyIterator() {
    return new ReusableIterator<ByteIterator>(map.keySet().iterator()) {
      @Override
      public ByteWritable next() {
        reusableKey.set(iter.nextByte());
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
    ObjectIterator<Byte2ObjectMap.Entry<V>> iterator =
        map.byte2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Byte2ObjectMap.Entry<V> entry = iterator.next();
      out.writeByte(entry.getByteKey());
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
      byte key = in.readByte();
      V value = valueWriter.readFields(in);
      map.put(key, value);
    }
  }
}
