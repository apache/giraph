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

import com.google.common.base.Joiner;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around {@link MetricsRegistry} to register metrics within a Giraph
 * job. After initializing, users can add {@link com.yammer.metrics.core.Metric}
 * and have it automatically piped into the configured outputs.
 */
public class GiraphMetrics {
  /** Enable the Metrics system **/
  public static final String ENABLE = "giraph.metrics.enable";

  /** Enable the metrics on the console **/
  public static final String CONSOLE_ENABLE = "giraph.metrics.console.enable";

  /** Time period for metrics **/
  public static final String CONSOLE_PERIOD = "giraph.metrics.console.period";

  /** @{link TimeUnit} for metrics time period **/
  public static final String CONSOLE_TIME_UNIT =
    "giraph.metrics.console.time.unit";

  /** Whether to dump all metrics when the job finishes */
  public static final String DUMP_AT_END = "giraph.metrics.dump.at.end";

  /** Use the Job ID as the group of the metrics */
  private static String JOB_ID = "";

  /** Has the metrics system be initialized? **/
  private static boolean INITED = false;

  /** The registry of metrics **/
  private static MetricsRegistry REGISTRY = new EmptyMetricsRegistry();
  /** The reporter for JMX **/
  private static JmxReporter REPORTER;

  /** Well, this is a private constructor... **/
  private GiraphMetrics() { }

  /**
   * Initialize the GiraphMetrics
   *
   * @param context Mapper's context
   */
  public static synchronized void init(Mapper.Context context) {
    if (INITED) {
      return;
    }

    Configuration conf = context.getConfiguration();
    if (conf.getBoolean(ENABLE, true)) {
      REGISTRY = new MetricsRegistry();
      REPORTER = new JmxReporter(REGISTRY);
      REPORTER.start();
      JOB_ID = context.getJobID().toString();
      initConsole(conf);
    }

    INITED = true;
  }

  /**
   * Initialize console printing
   *
   * @param conf Configuration object used by this job
   */
  private static void initConsole(Configuration conf) {
    if (conf.getBoolean(CONSOLE_ENABLE, false)) {
      String timeUnitString = conf.get(CONSOLE_TIME_UNIT, "SECONDS");
      TimeUnit timeUnit;
      try {
        timeUnit = TimeUnit.valueOf(timeUnitString.toUpperCase());
      } catch (IllegalArgumentException iae) {
        String values = Joiner.on(",").join(TimeUnit.values());
        throw new IllegalArgumentException("Unable to parse " + timeUnitString +
            " as value for " + CONSOLE_TIME_UNIT + ". Must be " +
            "one of: " + values);
      }

      int period = conf.getInt(CONSOLE_PERIOD, 90);
      ConsoleReporter.enable(REGISTRY, period, timeUnit);
    }
  }

  /**
   * Dump all metrics to output stream provided.
   *
   * @param out PrintStream to dump to.
   */
  public static void dumpToStream(PrintStream out) {
    new ConsoleReporter(REGISTRY, out, MetricPredicate.ALL).run();
  }

  /**
   * Dump all metrics to stdout.
   */
  public static void dumpToStdout() {
    dumpToStream(System.out);
  }

  /**
   * Create a MetricName using the job ID, group, and name.
   * @param group what type of metric this is
   * @param name String name given to metric
   * @return MetricName for use with MetricsRegistry
   */
  private static MetricName makeMetricName(MetricGroup group, String name) {
    return new MetricName(JOB_ID, group.toString().toLowerCase(), name);
  }

  /**
   * Creates a new {@link Counter} and registers it under the given group
   * and name.
   *
   * @param group what type of metric this is
   * @param name the name of the metric
   * @return a new {@link Counter}
   */
  public static Counter getCounter(MetricGroup group, String name) {
    return REGISTRY.newCounter(makeMetricName(group, name));
  }

  /**
   * Given a new {@link Gauge}, registers it under the given group and name.
   *
   * @param group  what type of metric this is
   * @param name   the name of the metric
   * @param metric the metric
   * @param <T>    the type of the value returned by the metric
   * @return {@code metric}
   */
  public static <T> Gauge<T> getGauge(MetricGroup group, String name,
                                      Gauge<T> metric) {
    return REGISTRY.newGauge(makeMetricName(group, name), metric);
  }

  /**
   * Creates a new non-biased {@link Histogram} and registers it under the given
   * group and name.
   *
   * @param group what type of metric this is
   * @param name  the name of the metric
   * @return a new {@link Histogram}
   */
  public static Histogram getHistogram(MetricGroup group, String name) {
    return REGISTRY.newHistogram(makeMetricName(group, name), false);
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
  public static Histogram getHistogram(MetricGroup group, String name,
                                       boolean biased) {
    return REGISTRY.newHistogram(makeMetricName(group, name), biased);
  }

  /**
   * Creates a new {@link Meter} and registers it under the given group
   * and name.
   *
   * @param group     what type of metric this is
   * @param name      the name of the metric
   * @param eventType the plural name of the type of events the meter is
   *                  measuring (e.g., {@code "requests"})
   * @param timeUnit  the rate unit of the new meter
   * @return a new {@link Meter}
   */
  public static Meter getMeter(MetricGroup group, String name, String eventType,
                               TimeUnit timeUnit) {
    return REGISTRY.newMeter(makeMetricName(group, name), eventType, timeUnit);
  }

  /**
   * Creates a new {@link Timer} and registers it under the given group and
   * name, measuring elapsed time in milliseconds and invocations per second.
   *
   * @param group what type of metric this is
   * @param name  the name of the metric
   * @return a new {@link Timer}
   */
  public static Timer getTimer(MetricGroup group, String name) {
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
  public static Timer getTimer(MetricGroup group, String name,
                               TimeUnit durationUnit, TimeUnit rateUnit) {
    return REGISTRY.newTimer(makeMetricName(group, name),
                             durationUnit, rateUnit);
  }
}
