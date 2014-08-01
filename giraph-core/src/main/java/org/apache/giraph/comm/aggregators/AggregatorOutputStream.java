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

import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.utils.WritableFactory;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of {@link CountingOutputStream} which allows writing of
 * aggregators in the form of triple (name, classname, value)
 */
public class AggregatorOutputStream extends CountingOutputStream {
  /**
   * Write aggregator to the stream and increment internal counter
   *
   * @param aggregatorName Name of the aggregator
   * @param aggregatorFactory Aggregator factory
   * @param aggregatedValue Value of aggregator
   * @return Number of bytes occupied by the stream
   * @throws IOException
   */
  public int addAggregator(String aggregatorName,
      WritableFactory<? extends Aggregator> aggregatorFactory,
      Writable aggregatedValue) throws IOException {
    incrementCounter();
    dataOutput.writeUTF(aggregatorName);
    WritableUtils.writeWritableObject(aggregatorFactory, dataOutput);
    aggregatedValue.write(dataOutput);
    return getSize();
  }
}
