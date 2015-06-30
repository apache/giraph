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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.io.Writable;
import org.python.google.common.base.Preconditions;

/**
 * Combines two individual reducers, to create a single reducer of pairs that
 * reduces each of them individually.
 *
 * @param <S1> First single value type
 * @param <R1> First reduced value type
 * @param <S2> Second single value type
 * @param <R2> Second reduced value type
 */
public class PairReduce<S1, R1 extends Writable, S2, R2 extends Writable>
    implements ReduceOperation<Pair<S1, S2>, PairWritable<R1, R2>> {
  /** First reduceOp */
  private ReduceOperation<S1, R1> reduce1;
  /** Second reduceOp */
  private ReduceOperation<S2, R2> reduce2;

  /** Constructor */
  public PairReduce() {
  }

  /**
   * Constructor
   * @param reduce1 First reduceOp
   * @param reduce2 Second reduceOp
   */
  public PairReduce(
      ReduceOperation<S1, R1> reduce1, ReduceOperation<S2, R2> reduce2) {
    this.reduce1 = reduce1;
    this.reduce2 = reduce2;
  }


  @Override
  public PairWritable<R1, R2> createInitialValue() {
    return new PairWritable<>(
        reduce1.createInitialValue(), reduce2.createInitialValue());
  }

  @Override
  public PairWritable<R1, R2> reduce(
      PairWritable<R1, R2> curValue, Pair<S1, S2> valueToReduce) {
    Preconditions.checkState(
        curValue.getLeft() ==
        reduce1.reduce(curValue.getLeft(), valueToReduce.getLeft()));
    Preconditions.checkState(
        curValue.getRight() ==
        reduce2.reduce(curValue.getRight(), valueToReduce.getRight()));
    return curValue;
  }

  @Override
  public PairWritable<R1, R2> reduceMerge(
      PairWritable<R1, R2> curValue, PairWritable<R1, R2> valueToReduce) {
    Preconditions.checkState(
        curValue.getLeft() ==
        reduce1.reduceMerge(curValue.getLeft(), valueToReduce.getLeft()));
    Preconditions.checkState(
        curValue.getRight() ==
        reduce2.reduceMerge(curValue.getRight(), valueToReduce.getRight()));
    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeWritableObject(reduce1, out);
    WritableUtils.writeWritableObject(reduce2, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reduce1 = WritableUtils.readWritableObject(in, null);
    reduce2 = WritableUtils.readWritableObject(in, null);
  }
}
