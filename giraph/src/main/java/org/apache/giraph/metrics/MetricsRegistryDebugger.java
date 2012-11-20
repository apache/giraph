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

import org.apache.log4j.Logger;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistryListener;

/**
 * A simple MetricsRegistry listener that prints every Metric added/removed.
 */
public class MetricsRegistryDebugger implements MetricsRegistryListener {
  /** logger instance */
  private static final Logger LOG =
      Logger.getLogger(MetricsRegistryDebugger.class);
  /** static singleton instance */
  private static final MetricsRegistryDebugger INSTANCE =
      new MetricsRegistryDebugger();

  /**
   * Get singleton static instance of this class.
   *
   * @return singleton static instance.
   */
  public static MetricsRegistryDebugger get() {
    return INSTANCE;
  }

  @Override
  public void onMetricAdded(MetricName name, Metric metric) {
    LOG.info("Adding metric " + name + " - " +
        metric.getClass().getSimpleName());
  }

  @Override
  public void onMetricRemoved(MetricName name) {
    LOG.info("Removing metric " + name);
  }
}
