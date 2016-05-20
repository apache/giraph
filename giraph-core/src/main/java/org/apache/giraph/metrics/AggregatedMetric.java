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
 *
 * @param <T> value type
 */
public abstract class AggregatedMetric<T extends Number> {
  /** Minimum value seen with the host that it came from */
  protected ValueWithHostname<T> min;
  /** Maximum value seen with the host that it came from */
  protected ValueWithHostname<T> max;
  /** Total of all the values seen */
  protected T sum;
  /** Number of values seen */
  protected long count;

  /**
   * Add another item to the aggregation.
   *
   * @param value value to add
   * @param hostnamePartitionId String hostname it came from
   */
  public abstract void addItem(T value, String hostnamePartitionId);

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
  public ValueWithHostname<T> min() {
    return min;
  }

  /**
   * Get maximum value together with host it came from.
   *
   * @return ValueWithHostname for maximum
   */
  public ValueWithHostname<T> max() {
    return max;
  }

  /**
   * Get total of all the values seen
   *
   * @return total of values seen
   */
  public T sum() {
    return sum;
  }

  /**
   * Get average of all the values
   *
   * @return computed average of all the values
   */
  public abstract double mean();

  /**
   * Get number of values seen
   *
   * @return count of values
   */
  public long count() {
    return count;
  }
}

