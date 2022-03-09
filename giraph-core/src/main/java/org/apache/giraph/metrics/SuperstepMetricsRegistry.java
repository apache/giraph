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

import java.io.PrintStream;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConfiguration;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.JmxReporter;

/**
 * Wrapper around MetricsRegistry for per-superstep metrics.
 */
public class SuperstepMetricsRegistry extends GiraphMetricsRegistry {
  /** Number of superstep to use for group of metrics created */
  private long superstep = BspService.INPUT_SUPERSTEP;

  /**
   * Constructor
   * @param registry {@link com.yammer.metrics.core.MetricsRegistry} to use
   * @param reporter {@link com.yammer.metrics.reporting.JmxReporter} to use
   * @param groupName String grouping for metrics
   * @param type String type name for metrics
   */
  protected SuperstepMetricsRegistry(MetricsRegistry registry,
      JmxReporter reporter, String groupName, String type) {
    super(registry, reporter, groupName, type);
  }

  /**
   * Create with Hadoop Configuration and superstep number.
   *
   * @param conf Hadoop Configuration to use.
   * @param superstep number of superstep to use as group for metrics.
   * @return new metrics registry
   */
  public static SuperstepMetricsRegistry create(GiraphConfiguration conf,
      long superstep) {
    if (conf.metricsEnabled()) {
      MetricsRegistry registry = new MetricsRegistry();
      SuperstepMetricsRegistry superstepMetrics = new SuperstepMetricsRegistry(
          registry, new JmxReporter(registry),
          "giraph.superstep", String.valueOf(superstep));
      superstepMetrics.superstep = superstep;
      return superstepMetrics;
    } else {
      return createFake();
    }
  }

  /**
   * Create an empty registry
   * @return fake metrics registry that returns no op metrics
   */
  public static SuperstepMetricsRegistry createFake() {
    return new SuperstepMetricsRegistry(new NoOpMetricsRegistry(), null,
        "", "");
  }

  /**
   * Get superstep stored here
   * @return long superstep
   */
  public long getSuperstep() {
    return superstep;
  }

  /**
   * Set superstep number used. Internally sets the group for metrics created.
   *
   * @param superstep long number of superstep to use.
   */
  public void setSuperstep(long superstep) {
    super.setType(String.valueOf(superstep));
    this.superstep = superstep;
  }

  /**
   * Print human readable summary of superstep metrics.
   *
   * @param out PrintStream to write to.
   */
  public void printSummary(PrintStream out) {
    new WorkerSuperstepMetrics().readFromRegistry().print(superstep, out);
    out.println("");
    MetricPredicate superstepFilter = new MetricPredicate() {
      @Override
      public boolean matches(MetricName name, Metric metric) {
        return name.getType().equals(getType());
      }
    };
    new ConsoleReporter(getInternalRegistry(), out, superstepFilter) {
      @Override
      public void processHistogram(MetricName name, Histogram histogram,
          PrintStream stream) {
        stream.printf("               sum = %,2.2f%n", histogram.sum());
        super.processHistogram(name, histogram, stream);
        stream.printf("             count = %d%n", histogram.count());
      }
    } .run();
  }
}
