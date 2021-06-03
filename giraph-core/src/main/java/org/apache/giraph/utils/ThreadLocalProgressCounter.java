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

import java.util.ArrayList;
import java.util.List;

/**
 * Makes a list of {@link ProgressCounter} accessible through
 * a {@link ThreadLocal}.
 */
public class ThreadLocalProgressCounter extends ThreadLocal<ProgressCounter> {
  /**
   * List of counters.
   */
  private final List<ProgressCounter> counters = new ArrayList<>();

  /**
   * Initializes a new counter, adds it to the list of counters
   * and returns it.
   * @return Progress counter.
   */
  @Override
  protected ProgressCounter initialValue() {
    ProgressCounter threadCounter = new ProgressCounter();
    synchronized (counters) {
      counters.add(threadCounter);
    }
    return threadCounter;
  }

  /**
   * Sums the progress of all counters.
   * @return Sum of all counters
   */
  public long getProgress() {
    long progress = 0;
    synchronized (counters) {
      for (ProgressCounter entry : counters) {
        progress += entry.getValue();
      }
    }
    return progress;
  }

  /**
   * Removes all counters.
   */
  public void reset() {
    counters.clear();
  }
}
