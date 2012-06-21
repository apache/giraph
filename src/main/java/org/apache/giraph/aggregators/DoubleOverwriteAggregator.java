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

import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator that stores a value that is overwritten once another value is
 * aggregated. This aggregator is useful for one-to-many communication from
 * master.compute() or from a special vertex. In case multiple vertices write
 * to this aggregator, its behavior is non-deterministic.
 */
public class DoubleOverwriteAggregator implements Aggregator<DoubleWritable> {
  /** Internal result */
  private double result = 0.0;

  /**
   * Aggregate with a primitive double.
   *
   * @param value Double value to aggregate.
   */
  public void aggregate(double value) {
    result = value;
  }

  @Override
  public void aggregate(DoubleWritable value) {
    result = value.get();
  }

  /**
   * Set aggregated value using a primitive double.
   *
   * @param value Double value to set.
   */
  public void setAggregatedValue(double value) {
    result = value;
  }

  @Override
  public void setAggregatedValue(DoubleWritable value) {
    result = value.get();
  }

  @Override
  public DoubleWritable getAggregatedValue() {
    return new DoubleWritable(result);
  }

  @Override
  public DoubleWritable createAggregatedValue() {
    return new DoubleWritable();
  }
}
