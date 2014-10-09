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
public class Reducer<S, R extends Writable> implements Writable {
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
    this.currentValue = reduceOp.createInitialValue();
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
  public void reduceSingle(S valueToReduce) {
    reduceOp.reduceSingle(currentValue, valueToReduce);
  }
  /**
   * Reduce given partially reduced value into current reduced value.
   * @param valueToReduce Partial value to reduce
   */
  public void reducePartial(R valueToReduce) {
    reduceOp.reducePartial(currentValue, valueToReduce);
  }
  /**
   * Return new initial reduced value.
   * @return New initial reduced value
   */
  public R createInitialValue() {
    return reduceOp.createInitialValue();
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

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeWritableObject(reduceOp, out);
    currentValue.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reduceOp = WritableUtils.readWritableObject(in, null);
    currentValue = reduceOp.createInitialValue();
    currentValue.readFields(in);
  }
}
