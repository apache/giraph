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

/**
 * An aggregator over metrics from multiple hosts. Computes min, max, and mean.
 */
public class AggregatedMetric {
  /** Minimum value seen with the host that it came from */
  private ValueWithHostname min;
  /** Maximum value seen with the host that it came from */
  private ValueWithHostname max;
  /** Total of all the values seen */
  private long sum;
  /** Number of values seen */
  private long count;

  /**
   * Create new aggregated metric.
   */
  public AggregatedMetric() {
    min = new ValueWithHostname(Long.MAX_VALUE);
    max = new ValueWithHostname(Long.MIN_VALUE);
  }

  /**
   * Add another item to the aggregation.
   *
   * @param value long value to add
   * @param hostnamePartitionId String hostname it came from
   */
  public void addItem(long value, String hostnamePartitionId) {
    if (value < min.getValue()) {
      min.set(value, hostnamePartitionId);
    }
    if (value > max.getValue()) {
      max.set(value, hostnamePartitionId);
    }
    sum += value;
    count++;
  }

  /**
   * Whether this AggregatedMetric has any data.
   *
   * @return true if we have any data.
   */
  public boolean hasData() {
    return count > 0;
  }

  /**
   * Get minimum value together with host it came from.
   *
   * @return ValueWithHostname for minimum
   */
  public ValueWithHostname min() {
    return min;
  }

  /**
   * Get maximum value together with host it came from.
   *
   * @return ValueWithHostname for maximum
   */
  public ValueWithHostname max() {
    return max;
  }

  /**
   * Get total of all the values seen
   *
   * @return long total of values seen
   */
  public long sum() {
    return sum;
  }

  /**
   * Get average of all the values
   *
   * @return computed average of all the values
   */
  public double mean() {
    return sum / (double) count;
  }

  /**
   * Get number of values seen
   *
   * @return count of values
   */
  public long count() {
    return count;
  }
}
