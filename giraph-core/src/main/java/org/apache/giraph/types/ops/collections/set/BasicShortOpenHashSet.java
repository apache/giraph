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
package org.apache.giraph.types.ops.collections.set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.types.ops.ShortTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.ShortWritable;

import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** ShortWritable implementation of BasicSet */
public final class BasicShortOpenHashSet
  implements BasicSet<ShortWritable> {
  /** Set */
  private final ShortSet set;

  /** Constructor */
  public BasicShortOpenHashSet() {
    set = new ShortOpenHashSet();
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   */
  public BasicShortOpenHashSet(int capacity) {
    set = new ShortOpenHashSet(capacity);
  }

  @Override
  public void clear() {
    set.clear();
  }

  @Override
  public long size() {
    return set.size();
  }

  @Override
  public void trim(long n) {
    ((ShortOpenHashSet) set).trim((int) Math.max(set.size(), n));
  }

  @Override
  public boolean add(ShortWritable value) {
    return set.add(value.get());
  }

  @Override
  public boolean contains(ShortWritable value) {
    return set.contains(value.get());
  }

  @Override
  public PrimitiveIdTypeOps<ShortWritable> getElementTypeOps() {
    return ShortTypeOps.INSTANCE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(set.size(), out);
    ShortIterator iter = set.iterator();
    while (iter.hasNext()) {
      out.writeShort(iter.nextShort());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = Varint.readUnsignedVarInt(in);
    set.clear();
    trim(size);
    for (int i = 0; i < size; ++i) {
      set.add(in.readShort());
    }
  }
}
