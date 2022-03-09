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

import org.apache.hadoop.io.Writable;

/**
 * Aggregates worker aggregator requests and sends them off
 */
public interface WorkerAggregatorRequestProcessor {
  /**
   * Sends worker reduced value to the owner of reducer
   *
   * @param name Name of the reducer
   * @param reducedValue Reduced partial value
   * @throws java.io.IOException
   * @return True if reduced value will be sent, false if this worker is
   * the owner of the reducer
   */
  boolean sendReducedValue(String name,
      Writable reducedValue) throws IOException;

  /**
   * Flush aggregated values cache.
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Sends reduced values to the master. This worker is the owner of these
   * reducers.
   *
   * @param data Serialized reduced values data
   * @throws IOException
   */
  void sendReducedValuesToMaster(byte[] data) throws IOException;

  /**
   * Sends reduced values to all other workers
   *
   * @param reducedDataList Serialized reduced values data split into chunks
   */
  void distributeReducedValues(
      Iterable<byte[]> reducedDataList) throws IOException;
}
