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
package org.apache.giraph.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import com.google.common.base.Preconditions;

/**
 * A Writable for arrays containing instances of a class. The elements of this
 * writable must all be instances of the same class.
 *
 * @param <T> element type
 */
public class ArrayWritable<T extends Writable> implements Writable {
  /** Element type class */
  private Class<T> valueClass;
  /** Array */
  private T[] values;

  /** Constructor */
  public ArrayWritable() {
  }

  /**
   * Constructor
   * @param valueClass Element type class
   * @param values Array of elements
   */
  public ArrayWritable(Class<T> valueClass, T[] values) {
    Preconditions.checkNotNull(valueClass,
        "valueClass cannot be null in ArrayWritable");
    this.valueClass = valueClass;
    this.values = values;
  }

  /**
   * Get element type class
   * @return element type class
   */
  public Class<T> getValueClass() {
    return valueClass;
  }

  /**
   * Set array
   * @param values array
   */
  public void set(T[] values) { this.values = values; }

  /**
   * Ger array
   * @return array
   */
  public T[] get() { return values; }

  @Override
  public void readFields(DataInput in) throws IOException {
    valueClass = WritableUtils.readClass(in);
    values = (T[]) Array.newInstance(valueClass, in.readInt());

    for (int i = 0; i < values.length; i++) {
      T value = (T) WritableFactories.newInstance(valueClass);
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(valueClass,
        "valueClass cannot be null in ArrayWritable");
    WritableUtils.writeClass(valueClass, out);
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }
}
