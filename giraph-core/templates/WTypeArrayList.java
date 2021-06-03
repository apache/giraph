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
import org.apache.giraph.function.primitive.${type.camel}Consumer;
import org.apache.giraph.function.primitive.${type.camel}Predicate;
import org.apache.giraph.types.ops.${type.camel}TypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.W${type.camel}Collection;
import org.apache.hadoop.io.${type.camel}Writable;
import org.apache.giraph.utils.Varint;

import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}ArrayList;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Arrays;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Collection;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}List;

${generated_message}

/**
 * Writable extension of ${type.camel}ArrayList, as well as
 * ${type.camel}Writable implementation of WArrayList.
 */
public class W${type.camel}ArrayList
    extends ${type.camel}ArrayList
    implements WArrayList<${type.camel}Writable>, W${type.camel}Collection {
  /**
   * Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity.
   */
  public W${type.camel}ArrayList() {
    super();
  }

  /**
   * Creates a new array list with given capacity.
   *
   * @param capacity the initial capacity of the array list (may be 0).
   */
  public W${type.camel}ArrayList(int capacity) {
    super(capacity);
  }

  /**
   * Creates a new array list and fills it with a given type-specific
   * collection.
   *
   * @param c a type-specific collection that will be used to fill the array
   *          list.
   */
  public W${type.camel}ArrayList(${type.camel}Collection c) {
    super(c);
  }

  /**
   * Creates a new array list and fills it with a given type-specific list.
   *
   * @param l a type-specific list that will be used to fill the array list.
   */
  public W${type.camel}ArrayList(${type.camel}List l) {
    super(l);
  }

  @Override
  public PrimitiveTypeOps<${type.camel}Writable> getElementTypeOps() {
    return ${type.camel}TypeOps.INSTANCE;
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
  public void addW(${type.camel}Writable value) {
    add(value.get());
  }

  @Override
  public void getIntoW(int index, ${type.camel}Writable to) {
    to.set(get${type.camel}(index));
  }

  @Override
  public void popIntoW(${type.camel}Writable to) {
    to.set(pop${type.camel}());
  }

  @Override
  public void setW(int index, ${type.camel}Writable value) {
    set(index, value.get());
  }

  @Override
  public void fillW(int from, int to, ${type.camel}Writable value) {
    if (to > size()) {
      throw new ArrayIndexOutOfBoundsException(
          "End index (" + to + ") is greater than array length (" +
              size() + ")");
    }
    Arrays.fill(elements(), from, to, value.get());
  }

  @Override
  public ResettableIterator<${type.camel}Writable> fastIteratorW() {
    return fastIteratorW(getElementTypeOps().create());
  }

  @Override
  public ResettableIterator<${type.camel}Writable> fastIteratorW(
      ${type.camel}Writable iterationValue) {
    return WArrayListPrivateUtils.fastIterator(this, iterationValue);
  }

  @Override
  public void fastForEachW(Consumer<${type.camel}Writable> f) {
    WArrayListPrivateUtils.fastForEach(this, f, getElementTypeOps().create());
  }

  @Override
  public boolean fastForEachWhileW(Predicate<${type.camel}Writable> f) {
    return WArrayListPrivateUtils.fastForEachWhile(
        this, f, getElementTypeOps().create());
  }

  /**
   * Traverse all elements of the array list, calling given function on each
   * element, or until predicate returns false.
   *
   * @param f Function to call on each element.
   */
  public void forEach${type.camel}(${type.camel}Consumer f) {
    for (int i = 0; i < size(); ++i) {
      f.apply(get${type.camel}(i));
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
  public boolean forEachWhile${type.camel}(${type.camel}Predicate f) {
    for (int i = 0; i < size(); ++i) {
      if (!f.apply(get${type.camel}(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void sort() {
    ${type.camel}Arrays.quickSort(elements(), 0, size());
  }

  @Override
  public void writeElements(DataOutput out) throws IOException {
    for (int i = 0; i < size; i++) {
      out.write${type.camel}(a[i]);
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
      a[i] = in.read${type.camel}();
    }
  }

  /**
   * Resize array for reading given number of elements.
   * @param size Number of elements that will be read.
   */
  protected void resizeArrayForRead(int size) {
    if (size != a.length) {
      a = new ${type.lower}[size];
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
  public static void writeOrNull(W${type.camel}ArrayList list, DataOutput out)
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
  public static W${type.camel}ArrayList readNew(DataInput in) throws IOException {
    int size = Varint.readSignedVarInt(in);
    W${type.camel}ArrayList list = new W${type.camel}ArrayList(size);
    list.readElements(in, size);
    return list;
  }

  /**
   * Variant of W${type.camel}ArrayList that doesn't reallocate smaller backing
   * array on consecutive readFields/readElements calls, and so is suitable for
   * reusable use.
   * (and backing array will only grow on readFields/readElements calls)
   */
  public static class WReusable${type.camel}ArrayList
      extends W${type.camel}ArrayList {
    /** Constructor */
    public WReusable${type.camel}ArrayList() {
      super();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public WReusable${type.camel}ArrayList(int capacity) {
      super(capacity);
    }

    @Override
    protected void resizeArrayForRead(int size) {
      if (size > a.length) {
        a = new ${type.lower}[size];
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
    public static WReusable${type.camel}ArrayList readIntoOrCreate(
        WReusable${type.camel}ArrayList list, DataInput in) throws IOException {
      int size = Varint.readUnsignedVarInt(in);
      if (list == null) {
        list = new WReusable${type.camel}ArrayList(size);
      }
      list.readElements(in, size);
      return list;
    }
  }
}
