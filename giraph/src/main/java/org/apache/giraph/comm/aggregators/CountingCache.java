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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Cache which counts the number of flushes per task id (destination worker
 * id), so we know how many requests were sent to the worker
 */
public abstract class CountingCache {
  /** Counts the number of flushes for each worker */
  private Map<Integer, Long> countMap = Maps.newHashMap();

  /**
   * Increase count of flushes for worker with desired task id. Subclasses
   * should call this method whenever flush is called.
   *
   * @param taskId Task id of worker
   */
  protected void incrementCounter(Integer taskId) {
    Long currentCount = countMap.get(taskId);
    countMap.put(taskId,
        (currentCount == null) ? 1 : (currentCount + 1));
  }

  /**
   * Get number of flushes for worker with desired task id
   *
   * @param taskId Task id of worker
   * @return Number of objects for the worker
   */
  protected long getCount(Integer taskId) {
    Long count = countMap.get(taskId);
    if (count == null) {
      return 0;
    } else {
      return count.longValue();
    }
  }

  /**
   * Reset the counts
   */
  public void reset() {
    countMap.clear();
  }
}
