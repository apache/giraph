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
package org.apache.giraph.types.ops.collections;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * BasicSet with only basic set of operations.
 * All operations that return object T are returning reusable object,
 * which is modified after calling any other function.
 *
 * @param <T> Element type
 */
public interface BasicSet<T> extends Writable {
  /** Removes all of the elements from this list. */
  void clear();
  /**
   * Number of elements in this list
   * @return size
   */
  int size();
  /**
   * Makes sure set is not using space with capacity more than
   * max(n,size()) entries.
   * @param n the threshold for the trimming.
   */
  void trim(int n);
  /**
   * Adds value to the set.
   * Returns <tt>true</tt> if set changed as a
   * result of the call.
   *
   * @param value Value to add
   * @return true if set was changed.
   */
  boolean add(T value);
  /**
   * Checks whether set contains given value
   * @param value Value to check
   * @return true if value is present in the set
   */
  boolean contains(T value);

  /**
   * TypeOps for type of elements this object holds
   * @return TypeOps
   */
  PrimitiveIdTypeOps<T> getElementTypeOps();

  /** IntWritable implementation of BasicSet */
  public static final class BasicIntOpenHashSet
      implements BasicSet<IntWritable> {
    /** Set */
    private final IntOpenHashSet set;

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicIntOpenHashSet(int capacity) {
      set = new IntOpenHashSet(capacity);
    }

    @Override
    public void clear() {
      set.clear();
    }

    @Override
    public int size() {
      return set.size();
    }

    @Override
    public void trim(int n) {
      set.trim(Math.max(set.size(), n));
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
      out.writeInt(set.size());
      IntIterator iter = set.iterator();
      while (iter.hasNext()) {
        out.writeInt(iter.nextInt());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      set.clear();
      set.trim(size);
      for (int i = 0; i < size; ++i) {
        set.add(in.readInt());
      }
    }
  }

  /** LongWritable implementation of BasicSet */
  public static final class BasicLongOpenHashSet
      implements BasicSet<LongWritable> {
    /** Set */
    private final LongOpenHashSet set;

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicLongOpenHashSet(int capacity) {
      set = new LongOpenHashSet(capacity);
    }

    @Override
    public void clear() {
      set.clear();
    }

    @Override
    public int size() {
      return set.size();
    }

    @Override
    public void trim(int n) {
      set.trim(Math.max(set.size(), n));
    }

    @Override
    public boolean add(LongWritable value) {
      return set.add(value.get());
    }

    @Override
    public boolean contains(LongWritable value) {
      return set.contains(value.get());
    }

    @Override
    public PrimitiveIdTypeOps<LongWritable> getElementTypeOps() {
      return LongTypeOps.INSTANCE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(set.size());
      LongIterator iter = set.iterator();
      while (iter.hasNext()) {
        out.writeLong(iter.nextLong());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      set.clear();
      trim(size);
      for (int i = 0; i < size; ++i) {
        set.add(in.readLong());
      }
    }
  }
}
