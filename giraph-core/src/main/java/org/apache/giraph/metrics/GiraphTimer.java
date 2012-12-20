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
 * A timer to record duration of an event in a given TimeUnit.
 * GiraphTimer is actually just a single-value Yammer Gauge that has some
 * methods to make timing things easier.
 */
public class GiraphTimer extends ValueGauge<Long> {
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

  /** dimension to measure things by */
  private TimeUnit timeUnit;

  /**
   * Create new timer, add it to the registry.
   *
   * @param registry GiraphMetricsRegistry to add timer to
   * @param name String name of timer
   * @param timeUnit TimeUnit to measure in
   */
  public GiraphTimer(GiraphMetricsRegistry registry, String name,
                     TimeUnit timeUnit) {
    super(registry, name);
    this.timeUnit = timeUnit;
    set(0L);
  }

  /**
   * Get TimeUnit used.
   *
   * @return TimeUnit being used.
   */
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  /**
   * Begin timing an event.
   *
   * @return GiraphTimerContext. Use stop() to end timing the event.
   */
  public GiraphTimerContext time() {
    return new GiraphTimerContext(this);
  }

  /**
   * Set value from a given TimeUnit, converting to our TimeUnit.
   *
   * @param value long measurement taken.
   * @param valueTimeUnit TimeUnit measurement is in.
   * @return this
   */
  public GiraphTimer set(long value, TimeUnit valueTimeUnit) {
    set(timeUnit.convert(value, valueTimeUnit));
    return this;
  }

  /**
   * Get abbreviated string of TimeUnit.
   *
   * @return TimeUnit abbreviation.
   */
  public String getTimeUnitAbbrev() {
    return TIME_UNIT_TO_ABBREV.get(timeUnit);
  }

  /**
   * Get string representation of value
   *
   * @return String value and abbreviated time unit
   */
  public String valueAndUnit() {
    return "" + value() + " " + getTimeUnitAbbrev();
  }
}
