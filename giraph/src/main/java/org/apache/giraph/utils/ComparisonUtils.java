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

import java.util.Iterator;

/** Simple helper class for comparisons and equality checking */
public class ComparisonUtils {

  /** Do not construct this object */
  private ComparisonUtils() { }

  /**
   * Compare elements, sort order and length
   *
   * @param <T> Type of iterable to compare.
   * @param first First iterable to compare.
   * @param second Second iterable to compare.
   * @return True if equal, false otherwise.
   */
  public static <T> boolean equal(Iterable<T> first, Iterable<T> second) {
    return equal(first.iterator(), second.iterator());
  }

  /**
   * Compare elements, sort order and length
   *
   * @param <T> Type of iterable to compare.
   * @param first First iterable to compare.
   * @param second Second iterable to compare.
   * @return True if equal, false otherwise.
   */
  public static <T> boolean equal(Iterator<T> first, Iterator<T> second) {
    while (first.hasNext() && second.hasNext()) {
      T message = first.next();
      T otherMessage = second.next();
      /* element-wise equality */
      if (!(message == null ? otherMessage == null :
        message.equals(otherMessage))) {
        return false;
      }
    }
    /* length must also be equal */
    return !(first.hasNext() || second.hasNext());
  }
}
