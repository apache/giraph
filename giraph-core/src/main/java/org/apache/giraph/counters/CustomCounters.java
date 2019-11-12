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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom counters created by the client or app
 * Stores the names of the counters, and the final aggregated values
 * Since there is no way to get the list of available counters from the context,
 * we need to call addCustomCounter to store the information about the list of
 * available counters. This is accessed by the worker/master to fetch the
 * final counter values based on the group and counter name.
 * It is assumed that whenever a counter is created, addCustomCounter is called,
 * to enable aggregations on that counter, using the Giraph mechanism for
 * aggregating counters.
 */
public class CustomCounters {



  /** Unique counter groups and names populated during execution of the job
   * Since multiple threads can access this variable to perform read/write
   * operations, we use a ConcurrentHashMap.newKeySet() to
   * establish synchronisation */
  private static Set<CustomCounter> COUNTER_NAMES =
          ConcurrentHashMap.newKeySet();

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
   * This will be called from the application to add the app-specific counters
   *
   * @param groupName Counter group
   * @param counterName Counter name
   * @param aggregation Type of aggregation for the counter
   */
  public static void addCustomCounter(String groupName, String counterName,
                                      CustomCounter.Aggregation aggregation) {
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
   * This will be called by the master when it gets all the counters from
   * different workers, and needs to aggregate them.
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
