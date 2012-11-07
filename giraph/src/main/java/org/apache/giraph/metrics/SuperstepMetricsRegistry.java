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

import org.apache.giraph.graph.BspService;
import org.apache.giraph.graph.BspServiceWorker;
import org.apache.giraph.graph.ComputeCallable;
import org.apache.giraph.graph.GraphMapper;
import org.apache.hadoop.conf.Configuration;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.Timer;

import java.io.PrintStream;

/**
 * Wrapper around MetricsRegistry for per-superstep metrics.
 */
public class SuperstepMetricsRegistry extends GiraphMetricsRegistry {
  /** Number of superstep to use for group of metrics created */
  private long superstep = BspService.INPUT_SUPERSTEP;

  /**
   * Create no-op registry that creates no-op metrics.
   */
  public SuperstepMetricsRegistry() {
    super();
  }

  /**
   * Create with Hadoop Configuration and superstep number.
   *
   * @param conf Hadoop Configuration to use.
   * @param superstep number of superstep to use as group for metrics.
   */
  public SuperstepMetricsRegistry(Configuration conf, long superstep) {
    super(conf, makeSuperstepGroupName(superstep));
    this.superstep = superstep;
  }

  /**
   * Set superstep number used. Internally sets the group for metrics created.
   *
   * @param superstep long number of superstep to use.
   */
  public void setSuperstep(long superstep) {
    super.setGroupName(makeSuperstepGroupName(superstep));
    this.superstep = superstep;
  }

  /**
   * Create group name to use for superstep.
   *
   * @param superstep long value of superstep to use.
   * @return String group for superstep to use for metrics created.
   */
  private static String makeSuperstepGroupName(long superstep) {
    return "giraph.superstep." + superstep;
  }

  /**
   * Print human readable summary of superstep metrics.
   *
   * @param out PrintStream to write to.
   */
  public void printSummary(PrintStream out) {
    Long commTime = getGaugeValue(MetricGroup.NETWORK,
        GraphMapper.GAUGE_COMMUNICATION_TIME);
    Long computeAllTime = getGaugeValue(MetricGroup.COMPUTE,
        GraphMapper.GAUGE_COMPUTE_ALL);
    Long timeToFirstMsg = getGaugeValue(MetricGroup.NETWORK,
        GraphMapper.GAUGE_TIME_TO_FIRST_MSG);
    Long superstepTime = getGaugeValue(MetricGroup.COMPUTE,
        GraphMapper.GAUGE_SUPERSTEP_TIME);
    Long waitingMs = getGaugeValue(MetricGroup.NETWORK,
        BspServiceWorker.GAUGE_WAITING_TIME);
    Timer computeOne = getTimer(MetricGroup.COMPUTE,
        ComputeCallable.TIMER_COMPUTE_ONE);
    double userComputeTime = computeOne.mean() * computeOne.count();

    out.println("");
    out.println("Superstep " + superstep + ":");
    out.println("  superstep time: " + superstepTime + " ms");
    out.println("  time to first message: " + timeToFirstMsg + " ms");
    out.println("  compute time: " + computeAllTime + " ms");
    out.println("  user compute time: " + userComputeTime + " ms");
    out.println("  network communication time: " + commTime + " ms");
    out.println("  waiting time: " + waitingMs + " ms");
  }

  /**
   * Print human readable summary of superstep metrics.
   */
  public void printSummary() {
    printSummary(System.out);
  }

  /**
   * Get a Gauge that is already present in the MetricsRegistry
   *
   * @param group MetricGroup Gauge belongs to
   * @param name String name of Gauge
   * @param <T> value type Gauge returns
   * @return Gauge<T> from MetricsRegistry
   */
  private <T> Gauge<T> getExistingGauge(MetricGroup group, String name) {
    Metric metric = getInternalRegistry().allMetrics().
        get(makeMetricName(group, name));
    return metric instanceof Gauge ? (Gauge<T>) metric : null;
  }

  /**
   * Get value of Gauge that is already present in the MetricsRegistry
   *
   * @param group MetricGroup Gauge belongs to
   * @param name String name of Gauge
   * @param <T> value type Gauge returns
   * @return T value of Gauge<T> from MetricsRegistry
   */
  private <T> T getGaugeValue(MetricGroup group, String name) {
    Gauge<T> gauge = getExistingGauge(group, name);
    return gauge == null ? null : gauge.value();
  }
}
