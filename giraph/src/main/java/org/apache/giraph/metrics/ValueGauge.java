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

import com.yammer.metrics.core.Gauge;

/**
 * A Gauge that holds a value.
 *
 * @param <T> type of value being held.
 */
public class ValueGauge<T extends Number> extends Gauge<T> {
  /** value held by this class */
  private T value;

  /**
   * Constructor that registers Gauge in MetricsRegistry.
   *
   * @param registry GiraphMetricsRegistry to use.
   * @param group MetricGroup for Gauge.
   * @param name String name of Gauge.
   */
  public ValueGauge(GiraphMetricsRegistry registry, MetricGroup group,
                    String name) {
    registry.getGauge(group, name, this);
  }

  @Override
  public T value() {
    return value;
  }

  /**
   * Get double representation of value held.
   *
   * @return double value
   */
  public double getDouble() {
    return value.doubleValue();
  }

  /**
   * Get long representation of value held.
   *
   * @return long value
   */
  public long getLong() {
    return value.longValue();
  }

  /**
   * Set value held by this object.
   *
   * @param value value to set.
   */
  public void set(T value) {
    this.value = value;
  }
}
