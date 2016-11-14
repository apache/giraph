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

import org.apache.giraph.conf.GiraphConfiguration;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.JmxReporter;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A holder for MetricsRegistry together with a JmxReporter.
 */
public class GiraphMetricsRegistry {
  /** String name of group to use for metrics created */
  private String groupName;
  /** String type to use for metrics created */
  private String type;
  /** Internal Yammer registry used */
  private final MetricsRegistry registry;
  /** JmxReporter that send metrics to JMX */
  private final JmxReporter jmxReporter;

  /**
   * Constructor
   * @param registry {@link MetricsRegistry} to use
   * @param reporter {@link JmxReporter} to use
   * @param groupName String grouping for metrics
   * @param type String type name for metrics
   */
  protected GiraphMetricsRegistry(MetricsRegistry registry,
      JmxReporter reporter, String groupName, String type) {
    this.registry = registry;
    this.jmxReporter = reporter;
    this.groupName = groupName;
    this.type = type;
    if (jmxReporter != null) {
      jmxReporter.start();
    }
  }

  /**
   * Create no-op empty registry that makes no-op metrics.
   * @return fake registry that makes no-op metrics
   */
  public static GiraphMetricsRegistry createFake() {
    return new GiraphMetricsRegistry(new NoOpMetricsRegistry(), null, "", "");
  }

  /**
   * Create registry with group to use for metrics.
   *
   * @param groupName String group to use for metrics.
   * @param type String type to use for metrics.
   * @return new metrics registry
   */
  public static GiraphMetricsRegistry createWithOptional(String groupName,
    String type) {
    MetricsRegistry registry = new MetricsRegistry();
    return new GiraphMetricsRegistry(registry, new JmxReporter(registry),
        groupName, type);
  }

  /**
   * Create registry with Hadoop Configuration and group to use for metrics.
   * Checks the configuration object for whether the optional metrics are
   * enabled, and optionally creates those.
   *
   * @param conf Hadoop Configuration to use.
   * @param groupName String group to use for metrics.
   * @param type String type to use for metrics.
   * @return new metrics registry
   */
  public static GiraphMetricsRegistry create(GiraphConfiguration conf,
    String groupName, String type) {
    if (conf.metricsEnabled()) {
      return createWithOptional(groupName, type);
    } else {
      return createFake();
    }
  }

  /**
   * Get map of all metrics.
   *
   * @return Map of all metrics held.
   */
  public Map<MetricName, Metric> getAll() {
    return registry.allMetrics();
  }

  /**
   * Get group name used for metrics.
   *
   * @return String group name.
   */
  public String getGroupName() {
    return groupName;
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
   * Get type used for new metrics created
   *
   * @return String type to use for metrics
   */
  public String getType() {
    return type;
  }

  /**
   * Set type to use for new metrics
   *
   * @param type String type to use
   */
  public void setType(String type) {
    this.type = type;
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
   * @param name the name of the metric
   * @return a new {@link com.yammer.metrics.core.Counter}
   */
  public Counter getCounter(String name) {
    return registry.newCounter(makeMetricName(name));
  }

  /**
   * Given a new {@link com.yammer.metrics.core.Gauge}, registers it under the
   * given group and name.
   *
   * @param name   the name of the metric
   * @param metric the metric
   * @param <T>    the type of the value returned by the metric
   * @return {@code metric}
   */
  public <T> Gauge<T> getGauge(String name, Gauge<T> metric) {
    return registry.newGauge(makeMetricName(name), metric);
  }

  /**
   * Creates a new biased {@link Histogram} and registers it under the given
   * group and name
   *
   * @param name name of metric
   * @return new {@link Histogram}
   */
  public Histogram getBiasedHistogram(String name) {
    return getHistogram(name, true);
  }

  /**
   * Creates a new uniform {@link Histogram} and registers it under the given
   * group and name
   *
   * @param name name of metric
   * @return new {@link Histogram}
   */
  public Histogram getUniformHistogram(String name) {
    return getHistogram(name, false);
  }

  /**
   * Creates a new {@link Histogram} and registers it under the given group
   * and name.
   *
   * @param name   the name of the metric
   * @param biased whether or not the histogram should be biased
   * @return a new {@link Histogram}
   */
  private Histogram getHistogram(String name, boolean biased) {
    return registry.newHistogram(makeMetricName(name), biased);
  }

  /**
   * Creates a new {@link com.yammer.metrics.core.Meter} and registers it under
   * the given group and name.
   *
   * @param meterDesc description of meter
   * @return new {@link com.yammer.metrics.core.Meter}
   */
  public Meter getMeter(MeterDesc meterDesc) {
    return getMeter(meterDesc.getName(), meterDesc.getType(),
        meterDesc.getTimeUnit());
  }

  /**
   * Creates a new {@link com.yammer.metrics.core.Meter} and registers it under
   * the given group and name.
   *
   * @param name      the name of the metric
   * @param eventType the plural name of the type of events the meter is
   *                  measuring (e.g., {@code "requests"})
   * @param timeUnit  the rate unit of the new meter
   * @return a new {@link com.yammer.metrics.core.Meter}
   */
  public Meter getMeter(String name, String eventType, TimeUnit timeUnit) {
    return registry.newMeter(makeMetricName(name), eventType, timeUnit);
  }

  /**
   * Creates a new {@link Timer} and registers it under the given
   * group and name.
   *
   * @param name         the name of the metric
   * @param durationUnit the duration scale unit of the new timer
   * @param rateUnit     the rate scale unit of the new timer
   * @return a new {@link Timer}
   */
  public Timer getTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return registry.newTimer(makeMetricName(name), durationUnit, rateUnit);
  }

  /**
   * Get a Gauge that is already present in the MetricsRegistry
   *
   * @param name String name of Gauge
   * @param <T> type of gauge
   * @return Gauge, from MetricsRegistry
   */
  public <T> Gauge<T> getExistingGauge(String name) {
    Metric metric = registry.allMetrics().get(makeMetricName(name));
    return metric instanceof Gauge ? (Gauge<T>) metric : null;
  }

  /**
   * Create a MetricName using the job ID, group, and name.
   *
   * @param name String name given to metric
   * @return MetricName for use with MetricsRegistry
   */
  protected MetricName makeMetricName(String name) {
    return new MetricName(groupName, type, name);
  }

  /**
   * Nothing will be captured after this is called.
   */
  public void shutdown() {
    registry.shutdown();
  }
}
