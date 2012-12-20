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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Takes and serializes aggregated values and keeps them grouped by owner
 * partition id, to be sent in bulk.
 */
public class SendAggregatedValueCache extends CountingCache {
  /** Map from worker partition id to aggregator output stream */
  private final Map<Integer, AggregatedValueOutputStream> aggregatorMap =
      Maps.newHashMap();

  /**
   * Add aggregated value to the cache
   *
   * @param taskId Task id of worker which owns the aggregator
   * @param aggregatorName Name of the aggregator
   * @param aggregatedValue Value of the aggregator
   * @return Number of bytes in serialized data for this worker
   * @throws IOException
   */
  public int addAggregator(Integer taskId, String aggregatorName,
      Writable aggregatedValue) throws IOException {
    AggregatedValueOutputStream out = aggregatorMap.get(taskId);
    if (out == null) {
      out = new AggregatedValueOutputStream();
      aggregatorMap.put(taskId, out);
    }
    return out.addAggregator(aggregatorName, aggregatedValue);
  }

  /**
   * Remove and get aggregators for certain worker
   *
   * @param taskId Partition id of worker owner
   * @return Serialized aggregator data for this worker
   */
  public byte[] removeAggregators(Integer taskId) {
    incrementCounter(taskId);
    AggregatedValueOutputStream out = aggregatorMap.remove(taskId);
    if (out == null) {
      return new byte[4];
    } else {
      return out.flush();
    }
  }

  /**
   * Creates fake aggregator which will hold the total number of aggregator
   * requests for worker with selected task id. This should be called after all
   * aggregators for the worker have been added to the cache.
   *
   * @param taskId Destination worker's task id
   * @throws IOException
   */
  public void addCountAggregator(Integer taskId) throws IOException {
    // current number of requests, plus one for the last flush
    long totalCount = getCount(taskId) + 1;
    addAggregator(taskId, AggregatorUtils.SPECIAL_COUNT_AGGREGATOR,
        new LongWritable(totalCount));
  }
}
