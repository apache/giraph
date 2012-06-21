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

import org.apache.hadoop.io.FloatWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for getting min float value.
 */
public class FloatMinAggregator implements Aggregator<FloatWritable> {
  /** Internal aggregator */
  private float min = Float.MAX_VALUE;

  /**
   * Aggregate with a primitive float.
   *
   * @param value Float value to aggregate.
   */
  public void aggregate(float value) {
    float val = value;
    if (val < min) {
      min = val;
    }
  }

  @Override
  public void aggregate(FloatWritable value) {
    float val = value.get();
    if (val < min) {
      min = val;
    }
  }

  /**
   * Set aggregated value using a primitive float.
   *
   * @param value Float value to set.
   */
  public void setAggregatedValue(float value) {
    min = value;
  }

  @Override
  public void setAggregatedValue(FloatWritable value) {
    min = value.get();
  }

  @Override
  public FloatWritable getAggregatedValue() {
    return new FloatWritable(min);
  }

  @Override
  public FloatWritable createAggregatedValue() {
    return new FloatWritable();
  }

}
