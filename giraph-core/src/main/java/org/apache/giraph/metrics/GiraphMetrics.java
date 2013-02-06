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
import org.apache.giraph.bsp.BspService;

import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;

/**
 * Top level metrics class for using Yammer's metrics in Giraph.
 */
public class GiraphMetrics {
  /** Singleton instance for everyone to use */
  private static GiraphMetrics INSTANCE = new GiraphMetrics();

  /** registry for per-superstep metrics */
  private final SuperstepMetricsRegistry perSuperstep;

  /** registry for per-job metrics */
  private final GiraphMetricsRegistry perJob;

  /** observer for per-superstep metrics re-initialization */
  private final List<ResetSuperstepMetricsObserver> observers =
      Lists.newArrayList();

  /**
   * Initialize no-op registry that creates no-op metrics.
   */
  private GiraphMetrics() {
    perJob = new GiraphMetricsRegistry();
    perSuperstep = new SuperstepMetricsRegistry();
  }

  /**
   * Initialize GiraphMetrics with Hadoop Context
   *
   * @param conf GiraphConfiguration to use.
   */
  private GiraphMetrics(GiraphConfiguration conf) {
    perJob = new GiraphMetricsRegistry(conf, "giraph", "job");
    perSuperstep = new SuperstepMetricsRegistry(conf,
        BspService.INPUT_SUPERSTEP);
  }

  /**
   * Get singleton instance of GiraphMetrics.
   *
   * @return GiraphMetrics singleton instance
   */
  public static GiraphMetrics get() {
    return INSTANCE;
  }

  /**
   * Initialize singleton instance of GiraphMetrics.
   *
   * @param conf GiraphConfiguration to use.
   */
  public static void init(GiraphConfiguration conf) {
    INSTANCE = new GiraphMetrics(conf);
  }

  /**
   * Get per-job metrics.
   *
   * @return per-job GiraphMetricsRegistry
   */
  public GiraphMetricsRegistry perJob() {
    return perJob;
  }

  /**
   * Get per-superstep metrics.
   *
   * @return per-superstep GiraphMetricsRegistry
   */
  public SuperstepMetricsRegistry perSuperstep() {
    return perSuperstep;
  }

  /**
   * Anyone using per-superstep counters needs to re-initialize their Metrics
   * object on each new superstep. Otherwise they will always be updating just
   * one counter. This method allows people to easily register a callback for
   * when they should do the re-initializing.
   *
   * @param observer SuperstepObserver to watch
   */
  public void addSuperstepResetObserver(
      ResetSuperstepMetricsObserver observer) {
    observers.add(observer);
  }

  /**
   * Reset the per-superstep MetricsRegistry
   *
   * @param superstep long number of superstep
   */
  public void resetSuperstepMetrics(long superstep) {
    perSuperstep.setSuperstep(superstep);
    for (ResetSuperstepMetricsObserver observer : observers) {
      observer.newSuperstep(perSuperstep);
    }
  }

  /**
   * Dump all metrics to output stream provided.
   *
   * @param out PrintStream to dump to.
   */
  public void dumpToStream(PrintStream out) {
    perJob.printToStream(out);
    perSuperstep.printToStream(out);
  }
}
