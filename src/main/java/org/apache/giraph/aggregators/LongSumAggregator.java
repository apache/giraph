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

package org.apache.giraph.aggregators;

import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for summing up long values.
 */
public class LongSumAggregator implements Aggregator<LongWritable> {
  /** Internal sum */
  private long sum = 0;

  /**
   * Aggregate a primitive long.
   *
   * @param value Long value to aggregate.
   */
  public void aggregate(long value) {
    sum += value;
  }

  @Override
  public void aggregate(LongWritable value) {
    sum += value.get();
  }

  /**
   * Set aggregated value using a primitive long.
   *
   * @param value Long value to set.
   */
  public void setAggregatedValue(long value) {
    sum = value;
  }

  @Override
  public void setAggregatedValue(LongWritable value) {
    sum = value.get();
  }

  @Override
  public LongWritable getAggregatedValue() {
    return new LongWritable(sum);
  }

  @Override
  public LongWritable createAggregatedValue() {
    return new LongWritable();
  }
}
