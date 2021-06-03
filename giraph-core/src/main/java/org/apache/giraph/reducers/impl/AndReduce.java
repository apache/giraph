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
import org.apache.hadoop.io.BooleanWritable;

/**
 *
 * ReduceOperation for calculating the AND function over boolean values.
 * The default value when nothing is aggregated is true.
 *
 */
public class AndReduce extends ReduceSameTypeOperation<BooleanWritable> {
  /** Instance */
  public static final AndReduce INSTANCE = new AndReduce();

  @Override
  public BooleanWritable createInitialValue() {
    return new BooleanWritable(true);
  }

  @Override
  public BooleanWritable reduce(
      BooleanWritable curValue, BooleanWritable valueToReduce) {
    curValue.set(curValue.get() && valueToReduce.get());
    return curValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }
}
