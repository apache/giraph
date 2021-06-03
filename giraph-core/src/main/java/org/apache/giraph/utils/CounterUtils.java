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

package org.apache.giraph.utils;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/** Utility methods for dealing with counters */
public class CounterUtils {
  /** Milliseconds to sleep for while waiting for counter to appear */
  private static final int SLEEP_MSECS = 100;

  /** Do not instantiate */
  private CounterUtils() {
  }

  /**
   * Wait for a counter to appear in a group and then return the name of that
   * counter. If job finishes before counter appears, return null.
   *
   * @param job   Job
   * @param group Name of the counter group
   * @return Name of the counter inside of the group, or null if job finishes
   *         before counter appears
   */
  public static String waitAndGetCounterNameFromGroup(Job job, String group) {
    try {
      while (job.getCounters().getGroup(group).size() == 0) {
        if (job.isComplete()) {
          return null;
        }
        Thread.sleep(SLEEP_MSECS);
      }
      return job.getCounters().getGroup(group).iterator().next().getName();
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(
          "waitAndGetCounterNameFromGroup: Exception occurred", e);
    }
  }
}
