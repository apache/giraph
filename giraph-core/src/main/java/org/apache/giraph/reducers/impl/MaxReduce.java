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
package org.apache.giraph.reducers.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.reducers.ReduceSameTypeOperation;
import org.apache.giraph.types.ops.DoubleTypeOps;
import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Reducer for calculating max of values
 * @param <T> Value type
 */
public class MaxReduce<T extends WritableComparable>
    extends ReduceSameTypeOperation<T> {
  /** DoubleWritable specialization */
  public static final MaxReduce<DoubleWritable> DOUBLE =
      new MaxReduce<>(DoubleTypeOps.INSTANCE);
  /** LongWritable specialization */
  public static final MaxReduce<LongWritable> LONG =
      new MaxReduce<>(LongTypeOps.INSTANCE);
  /** IntWritable specialization */
  public static final MaxReduce<IntWritable> INT =
      new MaxReduce<>(IntTypeOps.INSTANCE);

  /** Value type operations */
  private NumericTypeOps<T> typeOps;

  /** Constructor used for deserialization only */
  public MaxReduce() {
  }

  /**
   * Constructor
   * @param typeOps Value type operations
   */
  public MaxReduce(NumericTypeOps<T> typeOps) {
    this.typeOps = typeOps;
  }

  @Override
  public T createInitialValue() {
    return typeOps.createMinNegativeValue();
  }

  @Override
  public T reduce(T curValue, T valueToReduce) {
    if (curValue.compareTo(valueToReduce) < 0) {
      typeOps.set(curValue, valueToReduce);
    }
    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TypeOpsUtils.writeTypeOps(typeOps, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    typeOps = TypeOpsUtils.readTypeOps(in);
  }
}
