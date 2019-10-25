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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Custom counters created by the client or app
 * Stores the names of the counters, and the final aggregated values
 */
public class CustomCounters {

  /** Unique counter groups and names populated during execution of the job */
  private static Set<CustomCounter> COUNTER_NAMES = new HashSet<>();

  /** Aggregated counter values updated by master */
  private final List<CustomCounter> counterList;

  /**
   * Create custom counters
   */
  public CustomCounters() {
    counterList = new ArrayList<>();
  }

  /**
   * Add a counter group and name to the custom counters
   * If it already exists, then it will not be added
   *
   * @param groupName Counter group
   * @param counterName Counter name
   * @param aggregation Type of aggregation for the counter
   */
  public static void addCustomCounter(String groupName, String counterName,
                                      CustomCounter.AGGREGATION aggregation) {
    CustomCounter counter = new CustomCounter(
            groupName, counterName, aggregation);
    COUNTER_NAMES.add(counter);
  }

  /**
   * Get the unique counter group and names
   *
   * @return Map of unique counter names
   */
  public static Set<CustomCounter> getCustomCounters() {
    return COUNTER_NAMES;
  }

  /**
   * Merge the incoming counter values with the existing counters
   *
   * @param newCounters List of new counter names and values
   */
  public void mergeCounters(Set<CustomCounter> newCounters) {
    for (CustomCounter customCounter : newCounters) {
      int index = counterList.indexOf(customCounter);
      if (index == -1) {
        // counter is not present, then append to list
        counterList.add(customCounter);
      } else {
        long newValue = customCounter.getValue();
        long oldValue = counterList.get(index).getValue();
        CustomCounter oldCounter = counterList.get(index);
        switch (customCounter.getAggregation()) {
        case SUM:
          newValue = newValue + oldValue;
          break;
        case MAX:
          newValue = Math.max(newValue, oldValue);
          break;
        case MIN:
          newValue = Math.min(newValue, oldValue);
          break;
        default:
          // no op
        }
        oldCounter.setValue(newValue);
      }
    }
  }

  /**
   * Get the counter list
   *
   * @return List of counter names and values
   */
  public List<CustomCounter> getCounterList() {
    return this.counterList;
  }
}
