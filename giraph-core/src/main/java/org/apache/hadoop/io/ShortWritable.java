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
package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A WritableComparable for ints. */
public class ShortWritable implements WritableComparable {
  /** value */
  private short value;

  /** Constructor */
  public ShortWritable() { }

  /**
   * Constructor
   * @param value value
   */
  public ShortWritable(short value) {
    set(value);
  }

  /**
   * Set the value of this ShortWritable.
   * @param value value
   */
  public void set(short value) {
    this.value = value;
  }

  /**
   * Return the value of this ShortWritable.
   * @return value
   */
  public short get() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readShort();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(value);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ShortWritable)) {
      return false;
    }
    ShortWritable other = (ShortWritable) o;
    return this.value == other.value;
  }

  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public int compareTo(Object o) {
    int thisValue = this.value;
    int thatValue = ((ShortWritable) o).value;
    return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
  }

  @Override
  public String toString() {
    return Short.toString(value);
  }

  /** A Comparator optimized for IntWritable. */
  public static class Comparator extends WritableComparator {
    /** Constructor */
    public Comparator() {
      super(ShortWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      short thisValue = (short) readUnsignedShort(b1, s1);
      short thatValue = (short) readUnsignedShort(b2, s2);
      return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new Comparator());
  }
}
