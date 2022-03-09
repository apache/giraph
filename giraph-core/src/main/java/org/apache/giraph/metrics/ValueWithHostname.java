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
 * Pair of value with host it came from.
 *
 * @param <T> types of value (either long or double)
 */
public class ValueWithHostname<T extends Number> {
  /** long value we're holding */
  private T value;
  /** host associated with value */
  private String hostname;

  /**
   * Create with initial value
   *
   * @param value initial value to use
   */
  public ValueWithHostname(T value) {
    this.value = value;
    this.hostname = null;
  }

  /**
   * @return value
   */
  public T getValue() {
    return value;
  }

  /**
   * @return String hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * Check if there is any hostname. Used as a flag that we have any data.
   *
   * @return true if hostname is set
   */
  public boolean hasHostname() {
    return hostname != null;
  }

  /**
   * Set value and partition together.
   * @param value value to use.
   * @param hostname String host it came from.
   */
  public void set(T value, String hostname) {
    this.value = value;
    this.hostname = hostname;
  }
}
