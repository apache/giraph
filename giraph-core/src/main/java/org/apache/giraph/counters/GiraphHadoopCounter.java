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

package org.apache.giraph.counters;

import com.google.common.base.Objects;
import org.apache.hadoop.mapreduce.Counter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper around Hadoop Counter to make it easier to use.
 */
public class GiraphHadoopCounter {
  /** Hadoop Counter we're wrapping. */
  private Counter counter;

  /**
   * Create wrapping a Hadoop Counter.
   *
   * @param counter Hadoop Counter to wrap.
   */
  public GiraphHadoopCounter(Counter counter) {
    this.counter = counter;
  }

  /**
   * Get underlying Hadoop Counter we're wrapping.
   *
   * @return Hadoop Counter being wrapped.
   */
  public Counter getHadoopCounter() {
    return counter;
  }

  @Override
  public int hashCode() {
    return counter.hashCode();
  }

  @Override
  public boolean equals(Object genericRight) {
    if (genericRight == null) {
      return false;
    }
    if (getClass() != genericRight.getClass()) {
      return false;
    }
    GiraphHadoopCounter right = (GiraphHadoopCounter) genericRight;
    return Objects.equal(counter, right.counter);
  }

  /**
   * Set counter to value. Should be greater than current value.
   *
   * @param value long value to set to.
   */
  public void setValue(long value) {
    increment(value - getValue());
  }

  /**
   * Increment counter value by 1.
   */
  public void increment() {
    increment(1);
  }

  /**
   * Increment counter value.
   *
   * @param incr amount to increment by.
   */
  public void increment(long incr) {
    counter.increment(incr);
  }

  /**
   * Get counter value
   *
   * @return long value of counter
   */
  public long getValue() {
    return counter.getValue();
  }

  /**
   * Get counter display name.
   *
   * @return String Hadoop counter display name.
   */
  public String getDisplayName() {
    return counter.getDisplayName();
  }

  /**
   * Get counter name.
   *
   * @return String Hadoop counter name.
   */
  public String getName() {
    return counter.getName();
  }

  /**
   * Write to Hadoop output.
   *
   * @param out DataOutput to write to.
   * @throws IOException if something goes wrong.
   */
  public void write(DataOutput out) throws IOException {
    counter.write(out);
  }

  /**
   * Read from Hadoop input.
   *
   * @param in DataInput to read from.
   * @throws IOException if something goes wrong reading.
   */
  public void readFields(DataInput in) throws IOException {
    counter.readFields(in);
  }
}
