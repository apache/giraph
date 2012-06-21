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
 * Aggregator for calculating products of float values.
 */
public class FloatProductAggregator implements Aggregator<FloatWritable> {
  /** Aggregated product */
  private float product = 1.0f;

  /**
   * Aggregate a primitive float.
   *
   * @param value Float value to aggregate.
   */
  public void aggregate(float value) {
    product *= value;
  }

  @Override
  public void aggregate(FloatWritable value) {
    product *= value.get();
  }

  /**
   * Set aggregated value using a primitive float.
   *
   * @param value Float value to set.
   */
  public void setAggregatedValue(float value) {
    product = value;
  }

  @Override
  public void setAggregatedValue(FloatWritable value) {
    product = value.get();
  }

  @Override
  public FloatWritable getAggregatedValue() {
    return new FloatWritable(product);
  }

  @Override
  public FloatWritable createAggregatedValue() {
    return new FloatWritable();
  }

}
