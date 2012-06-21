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

import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for calculating the AND function over boolean values.
 * The default value when nothing is aggregated is true.
 */
public class BooleanAndAggregator implements Aggregator<BooleanWritable> {
  /** Internal result */
  private boolean result = true;

  /**
   * Aggregate with a primitive boolean.
   *
   * @param value Boolean value to aggregate.
   */
  public void aggregate(boolean value) {
    result = result && value;
  }

  @Override
  public void aggregate(BooleanWritable value) {
    result = result && value.get();
  }

  /**
   * Set aggregated value using a primitive boolean.
   *
   * @param value Boolean value to set.
   */
  public void setAggregatedValue(boolean value) {
    result = value;
  }

  @Override
  public void setAggregatedValue(BooleanWritable value) {
    result = value.get();
  }

  @Override
  public BooleanWritable getAggregatedValue() {
    return new BooleanWritable(result);
  }

  @Override
  public BooleanWritable createAggregatedValue() {
    return new BooleanWritable();
  }
}
