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

package org.apache.giraph.utils;

import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphMetricsRegistry;
import org.apache.giraph.metrics.MetricNames;

import com.yammer.metrics.util.PercentGauge;

/**
 * Helper static methods for tracking memory usage.
 */
public class MemoryUtils {
  /** Do not instantiate. */
  private MemoryUtils() { }

  /**
   * Helper to compute megabytes
   * @param bytes integer number of bytes
   * @return megabytes
   */
  private static double megaBytes(long bytes) {
    return bytes / 1024.0 / 1024.0;
  }

  /**
   * Get total memory in megabytes
   * @return total memory in megabytes
   */
  public static double totalMemoryMB() {
    return megaBytes(Runtime.getRuntime().totalMemory());
  }

  /**
   * Get maximum memory in megabytes
   * @return maximum memory in megabytes
   */
  public static double maxMemoryMB() {
    return megaBytes(Runtime.getRuntime().maxMemory());
  }

  /**
   * Get free memory in megabytes
   * @return free memory in megabytes
   */
  public static double freeMemoryMB() {
    return megaBytes(Runtime.getRuntime().freeMemory());
  }

  /**
   * Get free plus unallocated memory in megabytes
   * @return free plus unallocated memory in megabytes
   */
  public static double freePlusUnallocatedMemoryMB() {
    return freeMemoryMB() + maxMemoryMB() - totalMemoryMB();
  }

  /**
   * Get fraction of memory that's free
   * @return Fraction of memory that's free
   */
  public static double freeMemoryFraction() {
    return freePlusUnallocatedMemoryMB() / maxMemoryMB();
  }

  /**
   * Initialize metrics tracked by this helper.
   */
  public static void initMetrics() {
    GiraphMetricsRegistry metrics = GiraphMetrics.get().perJobOptional();
    metrics.getGauge(MetricNames.MEMORY_FREE_PERCENT, new PercentGauge() {
        @Override
        protected double getNumerator() {
          return freeMemoryMB();
        }

        @Override
        protected double getDenominator() {
          return totalMemoryMB();
        }
      }
    );
  }

  /**
   * Get stringified runtime memory stats
   *
   * @return String of all Runtime stats.
   */
  public static String getRuntimeMemoryStats() {
    return String.format("Memory (free/total/max) = %.2fM / %.2fM / %.2fM",
            freeMemoryMB(), totalMemoryMB(), maxMemoryMB());
  }
}
