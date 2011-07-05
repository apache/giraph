/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for getting max value.
 *
 **/

public class MaxAggregator implements Aggregator<DoubleWritable> {

  private double max = Double.MIN_VALUE;

  public void aggregate(DoubleWritable value) {
      double val = value.get();
      if (val > max) {
          max = val;
      }
  }

  public void setAggregatedValue(DoubleWritable value) {
      max = value.get();
  }

  public DoubleWritable getAggregatedValue() {
      return new DoubleWritable(max);
  }

  public DoubleWritable createAggregatedValue() {
      return new DoubleWritable();
  }
  
}
