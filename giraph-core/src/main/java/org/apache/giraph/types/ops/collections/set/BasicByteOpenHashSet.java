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

import org.apache.giraph.types.ops.ByteTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.ByteWritable;

import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteSet;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** ByteWritable implementation of BasicSet */
public final class BasicByteOpenHashSet
  implements BasicSet<ByteWritable> {
  /** Set */
  private final ByteSet set;

  /** Constructor */
  public BasicByteOpenHashSet() {
    set = new ByteOpenHashSet();
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   */
  public BasicByteOpenHashSet(int capacity) {
    set = new ByteOpenHashSet(capacity);
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
    ((ByteOpenHashSet) set).trim((int) Math.max(set.size(), n));
  }

  @Override
  public boolean add(ByteWritable value) {
    return set.add(value.get());
  }

  @Override
  public boolean contains(ByteWritable value) {
    return set.contains(value.get());
  }

  @Override
  public PrimitiveIdTypeOps<ByteWritable> getElementTypeOps() {
    return ByteTypeOps.INSTANCE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(set.size(), out);
    ByteIterator iter = set.iterator();
    while (iter.hasNext()) {
      out.writeByte(iter.nextByte());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = Varint.readUnsignedVarInt(in);
    set.clear();
    trim(size);
    for (int i = 0; i < size; ++i) {
      set.add(in.readByte());
    }
  }
}
