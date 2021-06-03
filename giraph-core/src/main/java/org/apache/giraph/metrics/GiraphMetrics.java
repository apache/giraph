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
import org.apache.giraph.utils.GcTracker;

import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;

import static org.apache.giraph.bsp.BspService.INPUT_SUPERSTEP;

/**
 * Top level metrics class for using Yammer's metrics in Giraph.
 */
public class GiraphMetrics {
  /** Singleton instance for everyone to use */
  private static GiraphMetrics INSTANCE = new GiraphMetrics();

  /** registry for per-superstep metrics */
  private final SuperstepMetricsRegistry perSuperstep;

  /** registry for optional per-job metrics */
  private final GiraphMetricsRegistry perJobOptional;

  /** registry for required per-job metrics */
  private final GiraphMetricsRegistry perJobRequired;

  /** Garbage collection tracker */
  private final GcTracker gcTracker;

  /** observer for per-superstep metrics re-initialization */
  private final List<ResetSuperstepMetricsObserver> observers =
      Lists.newArrayList();

  /**
   * Initialize no-op registry that creates no-op metrics.
   */
  private GiraphMetrics() {
    perJobOptional = GiraphMetricsRegistry.createFake();
    perSuperstep = SuperstepMetricsRegistry.createFake();
    perJobRequired = GiraphMetricsRegistry.createWithOptional("giraph", "job");
    gcTracker = new GcTracker();
  }

  /**
   * Initialize GiraphMetrics with Hadoop Context
   *
   * @param conf GiraphConfiguration to use.
   */
  private GiraphMetrics(GiraphConfiguration conf) {
    perJobOptional = GiraphMetricsRegistry.create(conf, "giraph", "job");
    perSuperstep = SuperstepMetricsRegistry.create(conf, INPUT_SUPERSTEP);
    perJobRequired = GiraphMetricsRegistry.createWithOptional("giraph", "job");
    gcTracker = new GcTracker(conf);
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
   * Get per-job optional metrics.
   *
   * @return per-job optional {@link GiraphMetricsRegistry}
   */
  public GiraphMetricsRegistry perJobOptional() {
    return perJobOptional;
  }

  /**
   * Get per-job required metrics.
   *
   * @return per-job require {@link GiraphMetricsRegistry}
   */
  public GiraphMetricsRegistry perJobRequired() {
    return perJobRequired;
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
   * Get GC tracker
   *
   * @return Gc tracker
   */
  public GcTracker getGcTracker() {
    return gcTracker;
  }

  /**
   * Anyone using per-superstep counters needs to re-initialize their Metrics
   * object on each new superstep. Otherwise they will always be updating just
   * one counter. This method allows people to easily register a callback for
   * when they should do the re-initializing.
   *
   * @param observer SuperstepObserver to watch
   */
  public synchronized void addSuperstepResetObserver(
      ResetSuperstepMetricsObserver observer) {
    observers.add(observer);
  }

  /**
   * Reset the per-superstep MetricsRegistry
   *
   * @param superstep long number of superstep
   */
  public synchronized void resetSuperstepMetrics(long superstep) {
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
    perJobOptional.printToStream(out);
    perJobRequired.printToStream(out);
  }

  /**
   * Stop using metrics (for cleanup)
   */
  public void shutdown() {
    perJobOptional.shutdown();
    perJobRequired.shutdown();
    perSuperstep.shutdown();
  }
}
