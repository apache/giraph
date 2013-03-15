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

import org.apache.hadoop.io.Text;

/**
 * Aggregator with {@link Text} as its value which keeps appending text to it
 */
public class TextAppendAggregator extends BasicAggregator<Text> {
  @Override
  public void aggregate(Text value) {
    byte[] valueBytes = value.getBytes();
    getAggregatedValue().append(valueBytes, 0, valueBytes.length);
  }

  @Override
  public Text createInitialValue() {
    return new Text();
  }
}
