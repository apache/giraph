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

import org.apache.giraph.GiraphConfiguration;
import org.apache.hadoop.conf.Configuration;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.JmxReporter;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * A holder for MetricsRegistry together with a JmxReporter.
 */
public class GiraphMetricsRegistry {
  /** String name of group to use for metrics created */
  private String groupName;
  /** Internal Yammer registry used */
  private final MetricsRegistry registry;
  /** JmxReporter that send metrics to JMX */
  private final JmxReporter jmxReporter;

  /**
   * Create no-op empty registry that makes no-op metrics.
   */
  public GiraphMetricsRegistry() {
    registry = new NoOpMetricsRegistry();
    jmxReporter = null;
  }

  /**
   * Create registry with Hadoop Configuration and group to use for metrics.
   *
   * @param conf Hadoop Configuration to use.
   * @param groupName String group to use for metrics.
   */
  public GiraphMetricsRegistry(Configuration conf,  String groupName) {
    this.groupName = groupName;
    if (conf.getBoolean(GiraphConfiguration.METRICS_ENABLE, false)) {
      registry = new MetricsRegistry();
      jmxReporter = new JmxReporter(registry);
      jmxReporter.start();
    } else {
      registry = new NoOpMetricsRegistry();
      jmxReporter = null;
    }
  }

  /**
   * Set group name used by this MetricsRegistry. Used for incrementing
   * superstep number to create a new hierarchy of metrics per superstep.
   *
   * @param groupName String group name to use.
   */
  protected void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  /**
   * Dump all the metrics to the PrintStream provided.
   *
   * @param out PrintStream to write metrics to.
   */
  public void printToStream(PrintStream out) {
    out.println("");
    new ConsoleReporter(registry, out, MetricPredicate.ALL).run();
  }

  /**
   * Get internal MetricsRegistry used.
   *
   * @return MetricsRegistry being used.
   */
  protected MetricsRegistry getInternalRegistry() {
    return registry;
  }

  /**
   * Creates a new {@link com.yammer.metrics.core.Counter} and registers it
   * under the given group and name.
   *
   * @param group what type of metric this is
   * @param name the name of the metric
   * @return a new {@link com.yammer.metrics.core.Counter}
   */
  public Counter getCounter(MetricGroup group, String name) {
    return registry.newCounter(makeMetricName(group, name));
  }

  /**
   * Given a new {@link com.yammer.metrics.core.Gauge}, registers it under the
   * given group and name.
   *
   * @param group  what type of metric this is
   * @param name   the name of the metric
   * @param metric the metric
   * @param <T>    the type of the value returned by the metric
   * @return {@code metric}
   */
  public <T> Gauge<T> getGauge(MetricGroup group, String name,
                               Gauge<T> metric) {
    return registry.newGauge(makeMetricName(group, name), metric);
  }

  /**
   * Creates a new non-biased {@link com.yammer.metrics.core.Histogram} and
   * registers it under the given group and name.
   *
   * @param group what type of metric this is
   * @param name  the name of the metric
   * @return a new {@link com.yammer.metrics.core.Histogram}
   */
  public Histogram getHistogram(MetricGroup group, String name) {
    return registry.newHistogram(makeMetricName(group, name), false);
  }

  /**
   * Creates a new {@link Histogram} and registers it under the given group
   * and name.
   *
   * @param group what type of metric this is
   * @param name   the name of the metric
   * @param biased whether or not the histogram should be biased
   * @return a new {@link Histogram}
   */
  public Histogram getHistogram(MetricGroup group, String name,
                                boolean biased) {
    return registry.newHistogram(makeMetricName(group, name), biased);
  }

  /**
   * Creates a new {@link com.yammer.metrics.core.Meter} and registers it under
   * the given group and name.
   *
   * @param group     what type of metric this is
   * @param name      the name of the metric
   * @param eventType the plural name of the type of events the meter is
   *                  measuring (e.g., {@code "requests"})
   * @param timeUnit  the rate unit of the new meter
   * @return a new {@link com.yammer.metrics.core.Meter}
   */
  public Meter getMeter(MetricGroup group, String name, String eventType,
                        TimeUnit timeUnit) {
    return registry.newMeter(makeMetricName(group, name), eventType, timeUnit);
  }

  /**
   * Creates a new {@link com.yammer.metrics.core.Timer} and registers it under
   * the given group and name, measuring elapsed time in milliseconds and
   * invocations per second.
   *
   * @param group what type of metric this is
   * @param name  the name of the metric
   * @return a new {@link com.yammer.metrics.core.Timer}
   */
  public Timer getTimer(MetricGroup group, String name) {
    return getTimer(group, name, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  }

  /**
   * Creates a new {@link Timer} and registers it under the given
   * group and name.
   *
   * @param group what type of metric this is
   * @param name         the name of the metric
   * @param durationUnit the duration scale unit of the new timer
   * @param rateUnit     the rate scale unit of the new timer
   * @return a new {@link Timer}
   */
  public Timer getTimer(MetricGroup group, String name,
                        TimeUnit durationUnit, TimeUnit rateUnit) {
    return registry.newTimer(makeMetricName(group, name),
                             durationUnit, rateUnit);
  }

  /**
   * Create a MetricName using the job ID, group, and name.
   *
   * @param group what type of metric this is
   * @param name String name given to metric
   * @return MetricName for use with MetricsRegistry
   */
  protected MetricName makeMetricName(MetricGroup group, String name) {
    return new MetricName(groupName, group.toString().toLowerCase(), name);
  }
}
