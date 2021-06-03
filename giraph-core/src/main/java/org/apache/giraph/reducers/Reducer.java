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
package org.apache.giraph.reducers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Object responsible for performing reducing operation.
 * Simple wrapper of ReduceOperation object and current value holding
 * partially reduced result.
 *
 * @param <S> Single value type, objects passed on workers
 * @param <R> Reduced value type
 */
public class Reducer<S, R extends Writable> {
  /** Reduce operations */
  private ReduceOperation<S, R> reduceOp;
  /** Current (partially) reduced value*/
  private R currentValue;

  /**
   * Constructor
   */
  public Reducer() {
  }
  /**
   * Constructor
   * @param reduceOp Reduce operations
   */
  public Reducer(ReduceOperation<S, R> reduceOp) {
    this.reduceOp = reduceOp;
    this.currentValue = createInitialValue();
  }
  /**
   * Constructor
   * @param reduceOp Reduce operations
   * @param currentValue current reduced value
   */
  public Reducer(ReduceOperation<S, R> reduceOp, R currentValue) {
    this.reduceOp = reduceOp;
    this.currentValue = currentValue;
  }

  /**
   * Reduce given value into current reduced value.
   * @param valueToReduce Single value to reduce
   */
  public void reduce(S valueToReduce) {
    currentValue = reduceOp.reduce(currentValue, valueToReduce);
  }
  /**
   * Reduce given partially reduced value into current reduced value.
   * @param valueToReduce Partial value to reduce
   */
  public void reduceMerge(R valueToReduce) {
    currentValue = reduceOp.reduceMerge(currentValue, valueToReduce);
  }
  /**
   * Return new initial reduced value.
   * @return New initial reduced value
   */
  public R createInitialValue() {
    R value = reduceOp.createInitialValue();
    if (value == null) {
      throw new IllegalStateException(
          "Initial value for reducer cannot be null, but is for " + reduceOp);
    }
    return value;
  }

  public ReduceOperation<S, R> getReduceOp() {
    return reduceOp;
  }

  public R getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(R currentValue) {
    this.currentValue = currentValue;
  }

  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeWritableObject(reduceOp, out);
    currentValue.write(out);
  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   *
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @param conf Configuration
   * @throws IOException
   */
  public void readFields(DataInput in,
      ImmutableClassesGiraphConfiguration conf) throws IOException {
    reduceOp = WritableUtils.readWritableObject(in, conf);
    currentValue = createInitialValue();
    currentValue.readFields(in);
  }
}
