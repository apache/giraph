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

import com.google.common.collect.ImmutableMap;

import java.util.concurrent.TimeUnit;

/**
 * Pair of long,TimeUnit
 */
public class LongAndTimeUnit {
  /** Mapping from TimeUnit to abbreviation used for printing */
  private static final ImmutableMap<TimeUnit, String> TIME_UNIT_TO_ABBREV =
      ImmutableMap.<TimeUnit, String>builder().
          put(TimeUnit.DAYS, "days").
          put(TimeUnit.HOURS, "hours").
          put(TimeUnit.MICROSECONDS, "us").
          put(TimeUnit.MILLISECONDS, "ms").
          put(TimeUnit.MINUTES, "mins").
          put(TimeUnit.NANOSECONDS, "ns").
          put(TimeUnit.SECONDS, "secs").build();

  /** value held */
  private long value;
  /** TimeUnit part */
  private TimeUnit timeUnit;

  @Override
  public String toString() {
    if (timeUnit == null) {
      return String.valueOf(value);
    } else {
      return value + " " + TIME_UNIT_TO_ABBREV.get(timeUnit);
    }
  }

  /**
   * Get the value
   *
   * @return value
   */
  public long getValue() {
    return value;
  }

  /**
   * Set the value
   *
   * @param value to set
   */
  public void setValue(long value) {
    this.value = value;
  }

  /**
   * Get the TimeUnit
   *
   * @return TimeUnit
   */
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  /**
   * Set the TimeUnit
   * @param timeUnit to set
   */
  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }
}
