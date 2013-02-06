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
package org.apache.giraph.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Description for Timers used in Giraph
 */
public enum TimerDesc {

  /** Timer around Vertex#compute() */
  COMPUTE_ONE("compute-one", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  /** Name of timer */
  private final String name;
  /** Duration unit for timer */
  private final TimeUnit durationUnit;
  /** Time unit for timer */
  private final TimeUnit timeUnit;

  /**
   * Constructor
   * @param name String name of timer
   * @param durationUnit Duration unit of timer
   * @param timeUnit Time unit of timer
   */
  private TimerDesc(String name, TimeUnit durationUnit, TimeUnit timeUnit) {
    this.name = name;
    this.durationUnit = durationUnit;
    this.timeUnit = timeUnit;
  }

  /**
   * Get duration unit of timer
   * @return TimeUnit
   */
  public TimeUnit getDurationUnit() {
    return durationUnit;
  }

  /**
   * Get name of timer
   * @return String name
   */
  public String getName() {
    return name;
  }

  /**
   * Get time unit of timer
   * @return TimeUnit of timer
   */
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }
}
