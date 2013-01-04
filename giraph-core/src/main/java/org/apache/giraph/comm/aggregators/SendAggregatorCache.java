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

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Takes and serializes aggregators and keeps them grouped by owner
 * partition id, to be sent in bulk.
 */
public class SendAggregatorCache extends CountingCache {
  /** Map from worker partition id to aggregator output stream */
  private final Map<Integer, AggregatorOutputStream> aggregatorMap =
      Maps.newHashMap();

  /**
   * Add aggregator to the cache
   *
   * @param taskId Task id of worker which owns the aggregator
   * @param aggregatorName Name of the aggregator
   * @param aggregatorClass Class of the aggregator
   * @param aggregatedValue Value of the aggregator
   * @return Number of bytes in serialized data for this worker
   * @throws IOException
   */
  public int addAggregator(Integer taskId, String aggregatorName,
      Class<? extends Aggregator> aggregatorClass,
      Writable aggregatedValue) throws IOException {
    AggregatorOutputStream out = aggregatorMap.get(taskId);
    if (out == null) {
      out = new AggregatorOutputStream();
      aggregatorMap.put(taskId, out);
    }
    return out.addAggregator(aggregatorName, aggregatorClass,
        aggregatedValue);
  }

  /**
   * Remove and get aggregators for certain worker
   *
   * @param taskId Task id of worker owner
   * @return Serialized aggregator data for this worker
   */
  public byte[] removeAggregators(Integer taskId) {
    incrementCounter(taskId);
    AggregatorOutputStream out = aggregatorMap.remove(taskId);
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
        Aggregator.class, new LongWritable(totalCount));
  }
}
