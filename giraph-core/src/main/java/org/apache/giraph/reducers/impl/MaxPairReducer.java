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
import org.apache.giraph.types.ops.TypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Aggregating PairWritable&lt;L, R&gt;, by taking pair with
 * largest second value.
 *
 * @param <L> Type of the left value
 * @param <R> Type of the right value
 */
public class MaxPairReducer<L extends Writable, R extends WritableComparable>
    extends ReduceSameTypeOperation<PairWritable<L, R>> {

  /** Left value TypeOps */
  private TypeOps<L> leftTypeOps;
  /** Right value TypeOps */
  private TypeOps<R> rightTypeOps;

  /** Constructor used for deserialization only */
  public MaxPairReducer() {
  }

  /**
   * Constructor
   * @param leftTypeOps Left value TypeOps
   * @param rightTypeOps Right value TypeOps
   */
  public MaxPairReducer(TypeOps<L> leftTypeOps, TypeOps<R> rightTypeOps) {
    this.leftTypeOps = leftTypeOps;
    this.rightTypeOps = rightTypeOps;
  }

  @Override
  public PairWritable<L, R> reduce(
      PairWritable<L, R> curValue, PairWritable<L, R> valueToReduce) {
    if (valueToReduce.getRight().compareTo(curValue.getRight()) > 0) {
      leftTypeOps.set(curValue.getLeft(), valueToReduce.getLeft());
      rightTypeOps.set(curValue.getRight(), valueToReduce.getRight());
    }
    return curValue;
  }

  @Override
  public PairWritable<L, R> createInitialValue() {
    return new PairWritable<L, R>(
        leftTypeOps.create(), rightTypeOps.create());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TypeOpsUtils.writeTypeOps(leftTypeOps, out);
    TypeOpsUtils.writeTypeOps(rightTypeOps, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    leftTypeOps = TypeOpsUtils.readTypeOps(in);
    rightTypeOps = TypeOpsUtils.readTypeOps(in);
  }
}
