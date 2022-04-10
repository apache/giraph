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

import org.apache.giraph.types.ops.${type.camel}TypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.ReusableIterator;
import org.apache.hadoop.io.${type.camel}Writable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}2ObjectMap;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Iterator;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * ${type.camel}Writable implementation of Basic2ObjectMap.
 *
 * @param <V> Value type
 */
public final class Basic${type.camel}2ObjectOpenHashMap<V>
    extends Basic2ObjectMap<${type.camel}Writable, V> {
  /** Map */
  private final ${type.camel}2ObjectOpenHashMap<V> map;
  /** Value writer */
  private final WritableWriter<V> valueWriter;

  /**
   * Constructor
   *
   * @param valueWriter Writer of values
   */
  public Basic${type.camel}2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
    this.map = new ${type.camel}2ObjectOpenHashMap<>();
    this.valueWriter = valueWriter;
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   * @param valueWriter Writer of values
   */
  public Basic${type.camel}2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    this.map = new ${type.camel}2ObjectOpenHashMap<>(capacity);
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
  public boolean containsKey(${type.camel}Writable key) {
    return map.containsKey(key.get());
  }

  @Override
  public V put(${type.camel}Writable key, V value) {
    return map.put(key.get(), value);
  }

  @Override
  public V get(${type.camel}Writable key) {
    return map.get(key.get());
  }

  @Override
  public V remove(${type.camel}Writable key) {
    return map.remove(key.get());
  }

  @Override
  public PrimitiveIdTypeOps<${type.camel}Writable> getKeyTypeOps() {
    return ${type.camel}TypeOps.INSTANCE;
  }

  @Override
  public Iterator<${type.camel}Writable> fastKeyIterator() {
    return new ReusableIterator<${type.camel}Iterator>(map.keySet().iterator()) {
      @Override
      public ${type.camel}Writable next() {
        reusableKey.set(iter.next${type.camel}());
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
    ObjectIterator<${type.camel}2ObjectMap.Entry<V>> iterator =
        map.${type.lower}2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      ${type.camel}2ObjectMap.Entry<V> entry = iterator.next();
      out.write${type.camel}(entry.get${type.camel}Key());
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
      ${type.lower} key = in.read${type.camel}();
      V value = valueWriter.readFields(in);
      map.put(key, value);
    }
  }
}
