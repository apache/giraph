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

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

/** Helper methods for Collections */
public class CollectionUtils {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CollectionUtils.class);

  /** Do not instantiate. */
  private CollectionUtils() { }

  /**
   * If map already has a value associated with the key it adds values to that
   * value, otherwise it will put values to the map.  Do not reuse values.
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

  /**
   * Helper method to check if iterables are equal.  Supports the case
   * where the iterable next() returns a reused object.  We do assume that
   * iterator() produces the objects in the same order across repeated calls,
   * if the object doesn't change.   This is very expensive (n^2) and should
   * be used for testing only.
   *
   * @param first First iterable
   * @param second Second iterable
   * @param <T> Type to compare
   * @return True if equal, false otherwise
   */
  public static <T> boolean isEqual(Iterable<T> first, Iterable<T> second) {
    // Relies on elements from the iterator arriving in the same order.
    // For every element in first, check elements on the second iterable by
    // marking the ones seen that have been found.  Then ensure that all
    // the elements of the second have been seen as well.
    int firstSize = Iterables.size(first);
    int secondSize = Iterables.size(second);
    boolean[] usedSecondArray = new boolean[secondSize];
    Iterator<T> firstIterator = first.iterator();
    while (firstIterator.hasNext()) {
      T firstValue = firstIterator.next();
      boolean foundFirstValue = false;
      Iterator<T> secondIterator = second.iterator();
      for (int i = 0; i < usedSecondArray.length; ++i) {
        T secondValue = secondIterator.next();
        if (!usedSecondArray[i]) {
          if (firstValue.equals(secondValue)) {
            usedSecondArray[i] = true;
            foundFirstValue = true;
            break;
          }
        }
      }

      if (!foundFirstValue) {
        LOG.error("isEqual: Couldn't find element from first (" + firstValue +
            ") in second " + second + "(size=" + secondSize + ")");
        return false;
      }
    }

    Iterator<T> secondIterator = second.iterator();
    for (int i = 0; i < usedSecondArray.length; ++i) {
      T secondValue = secondIterator.next();
      if (!usedSecondArray[i]) {
        LOG.error("isEqual: Element " + secondValue + " (index " + i +
            ") in second " + second + "(size=" + secondSize +
            ") not found in " + first + " (size=" + firstSize + ")");
        return false;
      }
    }

    return true;
  }
}
