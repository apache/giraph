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
package org.apache.giraph.block_app.framework.piece.global_comm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Utility object with common primitive reduce operations,
 * without need to create reusable objects within the piece.
 */
public class ReduceUtilsObject {
  private final DoubleWritable reusableDouble = new DoubleWritable();
  private final FloatWritable reusableFloat = new FloatWritable();
  private final LongWritable reusableLong = new LongWritable();
  private final IntWritable reusableInt = new IntWritable();

  // utility functions:
  public void reduceDouble(
      ReducerHandle<DoubleWritable, ?> reduceHandle, double value) {
    DoubleWritable tmp = reusableDouble;
    tmp.set(value);
    reduceHandle.reduce(tmp);
  }

  public void reduceFloat(
      ReducerHandle<FloatWritable, ?> reduceHandle, float value) {
    FloatWritable tmp = reusableFloat;
    tmp.set(value);
    reduceHandle.reduce(tmp);
  }

  public void reduceLong(
      ReducerHandle<LongWritable, ?> reduceHandle, long value) {
    LongWritable tmp = reusableLong;
    tmp.set(value);
    reduceHandle.reduce(tmp);
  }

  public void reduceInt(ReducerHandle<IntWritable, ?> reduceHandle, int value) {
    IntWritable tmp = reusableInt;
    tmp.set(value);
    reduceHandle.reduce(tmp);
  }
}
