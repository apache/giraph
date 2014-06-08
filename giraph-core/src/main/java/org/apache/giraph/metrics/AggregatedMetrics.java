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

import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.worker.BspServiceWorker;

import com.google.common.collect.Maps;

import java.io.PrintStream;
import java.util.Map;

/**
 * Map of a bunch of aggregated metrics
 */
public class AggregatedMetrics {
  /** Mapping from name to aggregated metric */
  private Map<String, AggregatedMetric> metrics = Maps.newHashMap();

  /**
   * Add value from hostname for a metric.
   *
   * @param name String name of metric
   * @param value long value to track
   * @param hostnamePartitionId String host it came from
   * @return this
   */
  public AggregatedMetrics add(String name, long value,
                               String hostnamePartitionId) {
    AggregatedMetric aggregatedMetric = metrics.get(name);
    if (aggregatedMetric == null) {
      aggregatedMetric = new AggregatedMetric();
      metrics.put(name, aggregatedMetric);
    }
    aggregatedMetric.addItem(value, hostnamePartitionId);
    return this;
  }

  /**
   * Add metrics from worker.
   *
   * @param workerMetrics WorkerSuperstepMetrics from work
   * @param hostname String hostname of worker
   * @return this
   */
  public AggregatedMetrics add(WorkerSuperstepMetrics workerMetrics,
                               String hostname) {
    add(GraphTaskManager.TIMER_SUPERSTEP_TIME,
        workerMetrics.getSuperstepTimer(), hostname);
    add(GraphTaskManager.TIMER_COMMUNICATION_TIME,
        workerMetrics.getCommTimer(), hostname);
    add(GraphTaskManager.TIMER_COMPUTE_ALL,
        workerMetrics.getComputeAllTimer(), hostname);
    add(GraphTaskManager.TIMER_TIME_TO_FIRST_MSG,
        workerMetrics.getTimeToFirstMsg(), hostname);
    add(BspServiceWorker.TIMER_WAIT_REQUESTS,
        workerMetrics.getWaitRequestsTimer(), hostname);
    return this;
  }

  /**
   * Print the aggregated metrics to the stream provided.
   *
   * @param superstep long number of superstep.
   * @param out PrintStream to write to.
   * @return this
   */
  public AggregatedMetrics print(long superstep, PrintStream out) {
    AggregatedMetric superstepTime = get(GraphTaskManager.TIMER_SUPERSTEP_TIME);
    AggregatedMetric commTime = get(GraphTaskManager.TIMER_COMMUNICATION_TIME);
    AggregatedMetric computeAll = get(GraphTaskManager.TIMER_COMPUTE_ALL);
    AggregatedMetric timeToFirstMsg =
        get(GraphTaskManager.TIMER_TIME_TO_FIRST_MSG);
    AggregatedMetric waitRequestsMicros = get(
        BspServiceWorker.TIMER_WAIT_REQUESTS);

    out.println();
    out.println("--- METRICS: superstep " + superstep + " ---");
    printAggregatedMetric(out, "superstep time", "ms", superstepTime);
    printAggregatedMetric(out, "compute all partitions", "ms", computeAll);
    printAggregatedMetric(out, "network communication time", "ms", commTime);
    printAggregatedMetric(out, "time to first message", "us", timeToFirstMsg);
    printAggregatedMetric(out, "wait requests time", "us", waitRequestsMicros);

    return this;
  }

  /**
   * Print batch of lines for AggregatedMetric
   *
   * @param out PrintStream to write to
   * @param header String header to print.
   * @param timeUnit String time unit of metric
   * @param aggregatedMetric AggregatedMetric to write
   */
  private void printAggregatedMetric(PrintStream out, String header,
                                     String timeUnit,
                                     AggregatedMetric aggregatedMetric) {
    if (aggregatedMetric.hasData()) {
      out.println(header);
      out.println("  mean: " + aggregatedMetric.mean() + " " + timeUnit);
      printValueFromHost(out, "  slowest: ", timeUnit, aggregatedMetric.max());
      printValueFromHost(out, "  fastest: ", timeUnit, aggregatedMetric.min());
    } else {
      out.println(header + ": NO DATA");
    }
  }

  /**
   * Print a line for a value with the host it came from.
   *
   * @param out PrintStream to write to
   * @param prefix String to write at beginning
   * @param timeUnit String timeUnit of metric
   * @param vh ValueWithHostname to write
   */
  private void printValueFromHost(PrintStream out, String prefix,
                                  String timeUnit, ValueWithHostname vh) {
    out.println(prefix + vh.getValue() + ' ' + timeUnit +
        " from " + vh.getHostname());
  }

  /**
   * Get AggregatedMetric with given name.
   *
   * @param name String metric to lookup.
   * @return AggregatedMetric for given metric name.
   */
  public AggregatedMetric get(String name) {
    return metrics.get(name);
  }

  /**
   * Get map of all aggregated metrics.
   *
   * @return Map of all the aggregated metrics.
   */
  public Map<String, AggregatedMetric> getAll() {
    return metrics;
  }
}
