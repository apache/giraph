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
 * Utility methods for Time classes.
 */
public class Times {
  /** Do not instantiate */
  private Times() { }

  /**
   * Convenience method to get milliseconds since a previous milliseconds
   * point.
   *
   * @param time Time instance to use
   * @param previousMilliseconds Previous milliseconds
   * @return Milliseconds elapsed since the previous milliseconds
   */
  public static long getMillisecondsSince(Time time,
                                          long previousMilliseconds) {
    return time.getMilliseconds() - previousMilliseconds;
  }

  /**
   * Convenience method to get milliseconds since a previous milliseconds
   * point.
   *
   * @param time Time instance to use
   * @param previousMs Previous milliseconds
   * @return Milliseconds elapsed since the previous milliseconds
   */
  public static long getMsSince(Time time, long previousMs) {
    return getMillisecondsSince(time, previousMs);
  }

  /**
   * Convenience method to get nanoseconds since a previous nanoseconds
   * point.
   *
   * @param time Time instance to use
   * @param previousNanoseconds Previous nanoseconds
   * @return Nanoseconds elapsed since the previous nanoseconds
   */
  public static long getNanosecondsSince(Time time, long previousNanoseconds) {
    return time.getNanoseconds() - previousNanoseconds;
  }

  /**
   * Convenience method to get nanoseconds since a previous nanoseconds
   * point.
   *
   * @param time Time instance to use
   * @param previousNanos Previous nanoseconds
   * @return Nanoseconds elapsed since the previous nanoseconds
   */
  public static long getNanosSince(Time time, long previousNanos) {
    return getNanosecondsSince(time, previousNanos);
  }

  /**
   * Convenience method to get seconds since a previous seconds
   * point.
   *
   * @param time Time instance to use
   * @param previousSeconds Previous seconds
   * @return Seconds elapsed since the previous seconds
   */
  public static int getSecondsSince(Time time, int previousSeconds) {
    return time.getSeconds() - previousSeconds;
  }

  /**
   * Convenience method to get seconds since a previous seconds
   * point.
   *
   * @param time Time instance to use
   * @param previousSec Previous seconds
   * @return Seconds elapsed since the previous seconds
   */
  public static int getSecSince(Time time, int previousSec) {
    return getSecondsSince(time, previousSec);
  }
}
