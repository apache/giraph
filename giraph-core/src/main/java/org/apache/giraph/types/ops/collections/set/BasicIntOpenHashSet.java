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

import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.IntWritable;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashBigSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** IntWritable implementation of BasicSet */
public final class BasicIntOpenHashSet
  implements BasicSet<IntWritable> {
  /** Set */
  private final IntSet set;

  /** Constructor */
  public BasicIntOpenHashSet() {
    set = new IntOpenHashSet();
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   */
  public BasicIntOpenHashSet(long capacity) {
    if (capacity <= MAX_OPEN_HASHSET_CAPACITY) {
      set = new IntOpenHashSet((int) capacity);
    } else {
      set = new IntOpenHashBigSet(capacity);
    }
  }

  @Override
  public void clear() {
    set.clear();
  }

  @Override
  public long size() {
    if (set instanceof IntOpenHashBigSet) {
      return ((IntOpenHashBigSet) set).size64();
    }
    return set.size();
  }

  @Override
  public void trim(long n) {
    if (set instanceof IntOpenHashSet) {
      ((IntOpenHashSet) set).trim((int) Math.max(set.size(), n));
    } else {
      ((IntOpenHashBigSet) set).trim(Math.max(set.size(), n));
    }
  }

  @Override
  public boolean add(IntWritable value) {
    return set.add(value.get());
  }

  @Override
  public boolean contains(IntWritable value) {
    return set.contains(value.get());
  }

  @Override
  public PrimitiveIdTypeOps<IntWritable> getElementTypeOps() {
    return IntTypeOps.INSTANCE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarLong(size(), out);
    IntIterator iter = set.iterator();
    while (iter.hasNext()) {
      out.writeInt(iter.nextInt());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long size = Varint.readUnsignedVarLong(in);
    set.clear();
    trim(size);
    for (long i = 0; i < size; ++i) {
      set.add(in.readInt());
    }
  }
}
