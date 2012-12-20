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

/**
 * A pair of integers.
 */
public class IntPair {
  /** First element. */
  private int first;
  /** Second element. */
  private int second;

  /** Constructor.
   *
   * @param fst First element
   * @param snd Second element
   */
  public IntPair(int fst, int snd) {
    first = fst;
    second = snd;
  }

  /**
   * Get the first element.
   *
   * @return The first element
   */
  public int getFirst() {
    return first;
  }

  /**
   * Set the first element.
   *
   * @param first The first element
   */
  public void setFirst(int first) {
    this.first = first;
  }

  /**
   * Get the second element.
   *
   * @return The second element
   */
  public int getSecond() {
    return second;
  }

  /**
   * Set the second element.
   *
   * @param second The second element
   */
  public void setSecond(int second) {
    this.second = second;
  }
}
