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
  public SuperstepMetricsRegistry(GiraphConfiguration conf, long superstep) {
    super(conf, "giraph.superstep", String.valueOf(superstep));
    this.superstep = superstep;
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
  }
}
