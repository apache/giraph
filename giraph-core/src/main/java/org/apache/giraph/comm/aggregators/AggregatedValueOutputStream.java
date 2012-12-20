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

package org.apache.giraph.comm.aggregators;

import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * Implementation of {@link CountingOutputStream} which allows writing of
 * aggregator values in the form of pair (name, value)
 */
public class AggregatedValueOutputStream extends CountingOutputStream {
  /**
   * Write aggregator to the stream and increment internal counter
   *
   * @param aggregatorName Name of the aggregator
   * @param aggregatedValue Value of aggregator
   * @return Number of bytes occupied by the stream
   * @throws IOException
   */
  public int addAggregator(String aggregatorName,
      Writable aggregatedValue) throws IOException {
    incrementCounter();
    dataOutput.writeUTF(aggregatorName);
    aggregatedValue.write(dataOutput);
    return getSize();
  }
}
