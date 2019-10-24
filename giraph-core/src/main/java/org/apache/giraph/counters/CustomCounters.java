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

package org.apache.giraph.counters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Custom counters created by the client or app
 * Stores the names of the counters, and the final aggregated values
 */
public class CustomCounters {

  /** Unique counter groups and names populated during execution of the job */
  private static Map<String, Set<String>> COUNTER_NAMES = new HashMap<>();

  /** Aggregated counter values updated by master */
  private final Map<String, Map<String, Long>> counterMap;

  /**
   * Create custom counters
   */
  public CustomCounters() {
    counterMap = new HashMap<>();
  }

  /**
   * Add a counter group and name to the custom counters
   * If it already exists, then it will not be added
   *
   * @param groupName Counter group
   * @param counterName Counter name
   */
  public static void addCustomCounter(String groupName, String counterName) {
    Set<String> counters = COUNTER_NAMES.getOrDefault(
            groupName, new HashSet<>());
    counters.add(counterName);
    COUNTER_NAMES.put(groupName, counters);
  }

  /**
   * Get the unique counter group and names
   *
   * @return Map of unique counter names
   */
  public static Map<String, Set<String>> getCustomCounters() {
    return COUNTER_NAMES;
  }

  /**
   * Merge the incoming counter values with the existing counters
   * Currently, it only supports addition
   * TODO: extend to other aggregation functions
   *
   * @param newCounters Map of new counter names and values
   */
  public void mergeCounters(Map<String, Map<String, Long>> newCounters) {
    for (Map.Entry<String, Map<String, Long>> entry :
            newCounters.entrySet()) {
      String groupName = entry.getKey();
      Map<String, Long> counters = entry.getValue();
      if (this.counterMap.get(groupName) == null) {
        this.counterMap.put(groupName, counters);
      } else {
        for (Map.Entry<String, Long> counter : counters.entrySet()) {
          String counterName = counter.getKey();
          long newValue = counter.getValue();
          long oldValue = this.counterMap.get(groupName).getOrDefault(
                  counterName, 0L);
          this.counterMap.get(groupName).put(
                  counterName, newValue + oldValue);
        }
      }
    }
  }

  /**
   * Get the countermap
   *
   * @return Map of counter names and values
   */
  public Map<String, Map<String, Long>> getCounterMap() {
    return this.counterMap;
  }
}
