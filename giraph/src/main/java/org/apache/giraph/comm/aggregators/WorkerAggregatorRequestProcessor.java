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
 * Aggregates worker aggregator requests and sends them off
 */
public interface WorkerAggregatorRequestProcessor {
  /**
   * Sends worker aggregated value to the owner of aggregator
   *
   * @param aggregatorName Name of the aggregator
   * @param aggregatedValue Value of the aggregator
   * @throws java.io.IOException
   * @return True if aggregated value will be sent, false if this worker is
   * the owner of the aggregator
   */
  boolean sendAggregatedValue(String aggregatorName,
      Writable aggregatedValue) throws IOException;

  /**
   * Flush aggregated values cache.
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Sends aggregated values to the master. This worker is the owner of these
   * aggregators.
   *
   * @param aggregatorData Serialized aggregator data
   * @throws IOException
   */
  void sendAggregatedValuesToMaster(byte[] aggregatorData) throws IOException;

  /**
   * Sends aggregators to all other workers
   *
   * @param aggregatorDataList Serialized aggregator data split into chunks
   */
  void distributeAggregators(
      Iterable<byte[]> aggregatorDataList) throws IOException;
}
