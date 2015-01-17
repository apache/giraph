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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.giraph.types.ops.BooleanTypeOps;
import org.apache.giraph.types.ops.ByteTypeOps;
import org.apache.giraph.types.ops.DoubleTypeOps;
import org.apache.giraph.types.ops.FloatTypeOps;
import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * BasicArrayList with only basic set of operations.
 *
 * @param <T> Element type
 */
public abstract class BasicArrayList<T> implements Writable {
  /** Removes all of the elements from this list. */
  public abstract void clear();
  /**
   * Number of elements in this list
   * @return size
   */
  public abstract int size();
  /**
   * Sets the size of this list.
   *
   * <P>
   * If the specified size is smaller than the current size,
   * the last elements are discarded.
   * Otherwise, they are filled with 0/<code>null</code>/<code>false</code>.
   *
   * @param newSize the new size.
   */
  public abstract void size(int newSize);
  /**
   * Capacity of currently allocated memory
   * @return capacity
   */
  public abstract int capacity();
  /**
   * Forces allocated memory to hold exactly N values
   * @param n new capacity
   */
  public abstract void setCapacity(int n);
  /**
   * Add value to the end of the array
   * @param value Value
   */
  public abstract void add(T value);
  /**
   * Pop value from the end of the array, storing it into 'to' argument
   * @param to Object to store value into
   */
  public abstract void popInto(T to);
  /**
   * Get element at given index in the array, storing it into 'to' argument
   * @param index Index
   * @param to Object to store value into
   */
  public abstract void getInto(int index, T to);
  /**
   * Set element at given index in the array
   * @param index Index
   * @param value Value
   */
  public abstract void set(int index, T value);

  /**
   * Sets given range of elements to a specified value.
   *
   * @param from From index (inclusive)
   * @param to To index (exclusive)
   * @param value Value
   */
  public abstract void fill(int from, int to, T value);

  /**
   * Returns underlying primitive collection:
   * it.unimi.dsi.fastutil.{T}s.{T}ArrayList.
   *
   * Allows for direct access where primitive type is fixed.
   *
   * @return underlying primitive collection
   */
  public abstract Object unwrap();

  /**
   * TypeOps for type of elements this object holds
   * @return TypeOps
   */
  public abstract PrimitiveTypeOps<T> getElementTypeOps();

  /**
   * Fast iterator over BasicArrayList object, which doesn't allocate new
   * element for each returned element, and can be iterated multiple times
   * using reset().
   *
   * Object returned by next() is only valid until next() is called again,
   * because it is reused.
   *
   * @return RessettableIterator
   */
  public ResettableIterator<T> fastIterator() {
    return new ResettableIterator<T>() {
      private final T value = getElementTypeOps().create();
      private int pos;

      @Override
      public boolean hasNext() {
        return pos < size();
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        getInto(pos, value);
        pos++;
        return value;
      }

      @Override
      public void reset() {
        pos = 0;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** BooleanWritable implementation of BasicArrayList */
  public static final class BasicBooleanArrayList
      extends BasicArrayList<BooleanWritable> {
    /** List */
    private final BooleanArrayList list;

    /** Constructor */
    public BasicBooleanArrayList() {
      list = new BooleanArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicBooleanArrayList(int capacity) {
      list = new BooleanArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<BooleanWritable> getElementTypeOps() {
      return BooleanTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(BooleanWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, BooleanWritable to) {
      to.set(list.getBoolean(index));
    }

    @Override
    public void popInto(BooleanWritable to) {
      to.set(list.popBoolean());
    }

    @Override
    public void set(int index, BooleanWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, BooleanWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public BooleanArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeBoolean(list.getBoolean(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readBoolean());
      }
    }
  }

  /** ByteWritable implementation of BasicArrayList */
  public static final class BasicByteArrayList
      extends BasicArrayList<ByteWritable> {
    /** List */
    private final ByteArrayList list;

    /** Constructor */
    public BasicByteArrayList() {
      list = new ByteArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicByteArrayList(int capacity) {
      list = new ByteArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<ByteWritable> getElementTypeOps() {
      return ByteTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(ByteWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, ByteWritable to) {
      to.set(list.getByte(index));
    }

    @Override
    public void popInto(ByteWritable to) {
      to.set(list.popByte());
    }

    @Override
    public void set(int index, ByteWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, ByteWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public ByteArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeByte(list.getByte(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readByte());
      }
    }
  }

  /** IntWritable implementation of BasicArrayList */
  public static final class BasicIntArrayList
      extends BasicArrayList<IntWritable> {
    /** List */
    private final IntArrayList list;

    /** Constructor */
    public BasicIntArrayList() {
      list = new IntArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicIntArrayList(int capacity) {
      list = new IntArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<IntWritable> getElementTypeOps() {
      return IntTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(IntWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, IntWritable to) {
      to.set(list.getInt(index));
    }

    @Override
    public void popInto(IntWritable to) {
      to.set(list.popInt());
    }

    @Override
    public void set(int index, IntWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, IntWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public IntArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeInt(list.getInt(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readInt());
      }
    }
  }

  /** LongWritable implementation of BasicArrayList */
  public static final class BasicLongArrayList
      extends BasicArrayList<LongWritable> {
    /** List */
    private final LongArrayList list;

    /** Constructor */
    public BasicLongArrayList() {
      list = new LongArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicLongArrayList(int capacity) {
      list = new LongArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<LongWritable> getElementTypeOps() {
      return LongTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(LongWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, LongWritable to) {
      to.set(list.getLong(index));
    }

    @Override
    public void popInto(LongWritable to) {
      to.set(list.popLong());
    }

    @Override
    public void set(int index, LongWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, LongWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public LongArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeLong(list.getLong(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readLong());
      }
    }
  }

  /** FloatWritable implementation of BasicArrayList */
  public static final class BasicFloatArrayList
      extends BasicArrayList<FloatWritable> {
    /** List */
    private final FloatArrayList list;

    /** Constructor */
    public BasicFloatArrayList() {
      list = new FloatArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicFloatArrayList(int capacity) {
      list = new FloatArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<FloatWritable> getElementTypeOps() {
      return FloatTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(FloatWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, FloatWritable to) {
      to.set(list.getFloat(index));
    }

    @Override
    public void popInto(FloatWritable to) {
      to.set(list.popFloat());
    }

    @Override
    public void set(int index, FloatWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, FloatWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public FloatArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeFloat(list.getFloat(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readFloat());
      }
    }
  }

  /** DoubleWritable implementation of BasicArrayList */
  public static final class BasicDoubleArrayList
      extends BasicArrayList<DoubleWritable> {
    /** List */
    private final DoubleArrayList list;

    /** Constructor */
    public BasicDoubleArrayList() {
      list = new DoubleArrayList();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public BasicDoubleArrayList(int capacity) {
      list = new DoubleArrayList(capacity);
    }

    @Override
    public PrimitiveTypeOps<DoubleWritable> getElementTypeOps() {
      return DoubleTypeOps.INSTANCE;
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public void size(int newSize) {
      list.size(newSize);
    }

    @Override
    public int capacity() {
      return list.elements().length;
    }

    @Override
    public void setCapacity(int n) {
      if (n >= list.elements().length) {
        list.ensureCapacity(n);
      } else {
        list.trim(n);
      }
    }

    @Override
    public void add(DoubleWritable value) {
      list.add(value.get());
    }

    @Override
    public void getInto(int index, DoubleWritable to) {
      to.set(list.getDouble(index));
    }

    @Override
    public void popInto(DoubleWritable to) {
      to.set(list.popDouble());
    }

    @Override
    public void set(int index, DoubleWritable value) {
      list.set(index, value.get());
    }

    @Override
    public void fill(int from, int to, DoubleWritable value) {
      if (to > list.size()) {
        throw new ArrayIndexOutOfBoundsException(
            "End index (" + to + ") is greater than array length (" +
                list.size() + ")");
      }
      Arrays.fill(list.elements(), from, to, value.get());
    }

    @Override
    public DoubleArrayList unwrap() {
      return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        out.writeDouble(list.getDouble(i));
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      list.clear();
      list.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
        list.add(in.readDouble());
      }
    }
  }
}
