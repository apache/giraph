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

package org.apache.giraph.aggregators.matrix.dense;

import it.unimi.dsi.fastutil.floats.FloatArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * The float dense vector holds the values of a particular row.
 * See DoubleDenseVector for explanation on why the singleton is needed.
 */
public class FloatDenseVector implements Writable {
  /** The entries of the vector. */
  private final FloatArrayList entries = new FloatArrayList();
  /** If true, this vector is singleton */
  private boolean isSingleton = false;
  /** The index of the singleton */
  private int singletonIndex;
  /** The value of the singleton */
  private float singletonValue;

  /** Create a new vector with default size. */
  public FloatDenseVector() { }

  /**
   * Create a new vector with given size.
   *
   * @param size the size of the vector
   */
  public FloatDenseVector(int size) {
    ensureCapacity(size);
  }

  /**
   * Set the singleton index and value.
   *
   * @param index the index
   * @param value the value
   */
  public void setSingleton(int index, float value) {
    isSingleton = true;
    this.singletonIndex = index;
    this.singletonValue = value;
  }

  /**
   * Get the singleton index.
   *
   * @return the singleton index
   */
  public int getSingletonIndex() {
    return singletonIndex;
  }

  /**
   * Get the singleton value.
   *
   * @return the singleton value
   */
  public float getSingletonValue() {
    return singletonValue;
  }

  /**
   * Get a particular entry of the vector.
   *
   * @param i the entry
   * @return the value of the entry.
   */
  public float get(int i) {
    // The default value is 0.0
    if (i >= entries.size()) {
      return 0.0f;
    }
    return entries.getFloat(i);
  }

  /**
   * Set the given value to the entry with the index specified.
   *
   * @param i the entry
   * @param value the value to set to the entry
   */
  public void set(int i, float value) {
    entries.set(i, value);
  }

  /**
   * Add the vector specified. This is a vector addition that does an
   * element-by-element addition.
   *
   * @param other the vector to add.
   */
  public void add(FloatDenseVector other) {
    if (isSingleton) {
      throw new RuntimeException("Cannot add to singleton vector");
    }
    if (other.isSingleton) {
      ensureCapacity(other.singletonIndex + 1);
      entries.set(other.singletonIndex, entries.getFloat(other.singletonIndex) +
          other.singletonValue);
    } else {
      ensureCapacity(other.entries.size());
      for (int i = 0; i < other.entries.size(); ++i) {
        entries.set(i, entries.getFloat(i) + other.entries.getFloat(i));
      }
    }
  }

  /**
   * Resize the array to be at least the size specified.
   *
   * @param size the size of the array
   */
  private void ensureCapacity(int size) {
    if (entries.size() < size) {
      entries.size(size);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isSingleton);
    if (isSingleton) {
      out.writeInt(singletonIndex);
      out.writeFloat(singletonValue);
    } else {
      out.writeInt(entries.size());
      for (int i = 0; i < entries.size(); ++i) {
        out.writeFloat(entries.getFloat(i));
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    isSingleton = in.readBoolean();
    if (isSingleton) {
      singletonIndex = in.readInt();
      singletonValue = in.readFloat();
    } else {
      int size = in.readInt();
      for (int i = 0; i < size; ++i) {
        entries.add(in.readFloat());
      }
    }
  }
}
