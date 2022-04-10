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
package org.apache.giraph.types.ops.collections.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.ShortConsumer;
import org.apache.giraph.function.primitive.ShortPredicate;
import org.apache.giraph.types.ops.ShortTypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.WShortCollection;
import org.apache.hadoop.io.ShortWritable;
import org.apache.giraph.utils.Varint;

import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortCollection;
import it.unimi.dsi.fastutil.shorts.ShortList;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/**
 * Writable extension of ShortArrayList, as well as
 * ShortWritable implementation of WArrayList.
 */
public class WShortArrayList
    extends ShortArrayList
    implements WArrayList<ShortWritable>, WShortCollection {
  /**
   * Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity.
   */
  public WShortArrayList() {
    super();
  }

  /**
   * Creates a new array list with given capacity.
   *
   * @param capacity the initial capacity of the array list (may be 0).
   */
  public WShortArrayList(int capacity) {
    super(capacity);
  }

  /**
   * Creates a new array list and fills it with a given type-specific
   * collection.
   *
   * @param c a type-specific collection that will be used to fill the array
   *          list.
   */
  public WShortArrayList(ShortCollection c) {
    super(c);
  }

  /**
   * Creates a new array list and fills it with a given type-specific list.
   *
   * @param l a type-specific list that will be used to fill the array list.
   */
  public WShortArrayList(ShortList l) {
    super(l);
  }

  @Override
  public PrimitiveTypeOps<ShortWritable> getElementTypeOps() {
    return ShortTypeOps.INSTANCE;
  }

  @Override
  public int capacity() {
    return elements().length;
  }

  @Override
  public void setCapacity(int n) {
    if (n >= capacity()) {
      ensureCapacity(n);
    } else {
      trim(n);
    }
  }

  @Override
  public void addW(ShortWritable value) {
    add(value.get());
  }

  @Override
  public void getIntoW(int index, ShortWritable to) {
    to.set(getShort(index));
  }

  @Override
  public void popIntoW(ShortWritable to) {
    to.set(popShort());
  }

  @Override
  public void setW(int index, ShortWritable value) {
    set(index, value.get());
  }

  @Override
  public void fillW(int from, int to, ShortWritable value) {
    if (to > size()) {
      throw new ArrayIndexOutOfBoundsException(
          "End index (" + to + ") is greater than array length (" +
              size() + ")");
    }
    Arrays.fill(elements(), from, to, value.get());
  }

  @Override
  public ResettableIterator<ShortWritable> fastIteratorW() {
    return fastIteratorW(getElementTypeOps().create());
  }

  @Override
  public ResettableIterator<ShortWritable> fastIteratorW(
      ShortWritable iterationValue) {
    return WArrayListPrivateUtils.fastIterator(this, iterationValue);
  }

  @Override
  public void fastForEachW(Consumer<ShortWritable> f) {
    WArrayListPrivateUtils.fastForEach(this, f, getElementTypeOps().create());
  }

  @Override
  public boolean fastForEachWhileW(Predicate<ShortWritable> f) {
    return WArrayListPrivateUtils.fastForEachWhile(
        this, f, getElementTypeOps().create());
  }

  /**
   * Traverse all elements of the array list, calling given function on each
   * element, or until predicate returns false.
   *
   * @param f Function to call on each element.
   */
  @Override
  public void forEachShort(ShortConsumer f) {
    for (int i = 0; i < size(); ++i) {
      f.apply(getShort(i));
    }
  }

  /**
   * Traverse all elements of the array list, calling given function on each
   * element.
   *
   * @param f Function to call on each element.
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   */
  @Override
  public boolean forEachWhileShort(ShortPredicate f) {
    for (int i = 0; i < size(); ++i) {
      if (!f.apply(getShort(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void sort() {
    Arrays.sort(elements(), 0, size());
  }

  @Override
  public void swap(int i, int j) {
    int size = size();
    if (i >= size || j >= size) {
      throw new IndexOutOfBoundsException(
          "Index (" + Math.max(i,  j) +
          ") is greater than or equal to list size (" + size + ")");
    }
    short[] arr = elements();
    short tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }

  @Override
  public void writeElements(DataOutput out) throws IOException {
    for (int i = 0; i < size; i++) {
      out.writeShort(a[i]);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(size, out);
    writeElements(out);
  }

  @Override
  public void readElements(DataInput in, int size) throws IOException {
    this.size = size;
    resizeArrayForRead(size);
    for (int i = 0; i < size; i++) {
      a[i] = in.readShort();
    }
  }

  /**
   * Resize array for reading given number of elements.
   * @param size Number of elements that will be read.
   */
  protected void resizeArrayForRead(int size) {
    if (size != a.length) {
      a = new short[size];
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    readElements(in, Varint.readUnsignedVarInt(in));
  }

  /**
   * Write a potentially null list to a DataOutput. Null list is written
   * equivalently as an empty list. Use WritableUtils.writeIfNotNullAndObject
   * if distinction is needed.
   *
   * @param list Array list to be written
   * @param out Data output
   */
  public static void writeOrNull(WShortArrayList list, DataOutput out)
      throws IOException {
    if (list == null) {
      Varint.writeUnsignedVarInt(0, out);
    } else {
      list.write(out);
    }
  }

  /**
   * Read array list from the DataInput stream into a newly created object.
   *
   * @param in Data input
   * @return New read array list object.
   */
  public static WShortArrayList readNew(DataInput in) throws IOException {
    int size = Varint.readSignedVarInt(in);
    WShortArrayList list = new WShortArrayList(size);
    list.readElements(in, size);
    return list;
  }

  /**
   * Variant of WShortArrayList that doesn't reallocate smaller backing
   * array on consecutive readFields/readElements calls, and so is suitable for
   * reusable use.
   * (and backing array will only grow on readFields/readElements calls)
   */
  public static class WReusableShortArrayList
      extends WShortArrayList {
    /** Constructor */
    public WReusableShortArrayList() {
      super();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public WReusableShortArrayList(int capacity) {
      super(capacity);
    }

    @Override
    protected void resizeArrayForRead(int size) {
      if (size > a.length) {
        a = new short[size];
      }
    }

    /**
     * Read array list from DataInput stream, into a given list if not null,
     * or creating a new list if given list is null.
     *
     * @param list Array list to be written
     * @param in Data input
     * @return Passed array list, or a new one if null is passed
     */
    public static WReusableShortArrayList readIntoOrCreate(
        WReusableShortArrayList list, DataInput in) throws IOException {
      int size = Varint.readUnsignedVarInt(in);
      if (list == null) {
        list = new WReusableShortArrayList(size);
      }
      list.readElements(in, size);
      return list;
    }
  }
}
