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

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/** Helper methods for Collections */
public class CollectionUtils {
  /** Do not instantiate. */
  private CollectionUtils() { }

  /**
   * If map already has value associated with the key it adds values to that
   * value, otherwise it will put values to the map.
   *
   * @param key    Key under which we are adding values
   * @param values Values we want to add
   * @param map    Map which we are adding values to
   * @param <K> Key
   * @param <V> Value
   * @param <C> Collection
   * @return New value associated with the key
   */
  public static <K, V, C extends Collection<V>> C addConcurrent(K key,
      C values, ConcurrentMap<K, C> map) {
    C currentValues = map.get(key);
    if (currentValues == null) {
      currentValues = map.putIfAbsent(key, values);
      if (currentValues == null) {
        return values;
      }
    }
    synchronized (currentValues) {
      currentValues.addAll(values);
    }
    return currentValues;
  }
}
