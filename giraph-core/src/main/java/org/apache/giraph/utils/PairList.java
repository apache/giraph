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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Collection to keep pairs in, without creating a wrapper object around
 * each pair of objects.
 *
 * @param <U> Type of the first element in a pair
 * @param <V> Type of the second element in a pair
 */
public class PairList<U, V> {
  /** List to keep first elements of pairs in */
  protected List<U> firstList;
  /** List to keep second elements of pairs in */
  protected List<V> secondList;

  /**
   * Constructor
   */
  public PairList() {
  }

  /**
   * Initialize the inner state. Must be called before {@code add()} is
   * called.
   */
  public void initialize() {
    firstList = Lists.newArrayList();
    secondList = Lists.newArrayList();
  }


  /**
   * Initialize the inner state, with a known size. Must be called before
   * {@code add()} is called.
   *
   * @param size Number of pairs which will be added to the list
   */
  public void initialize(int size) {
    firstList = Lists.newArrayListWithCapacity(size);
    secondList = Lists.newArrayListWithCapacity(size);
  }

  /**
   * Add a pair to the collection.
   *
   * @param first First element of the pair
   * @param second Second element of the pair
   */
  public void add(U first, V second) {
    firstList.add(first);
    secondList.add(second);
  }

  /**
   * Get number of pairs in this list.
   *
   * @return Number of pairs in the list
   */
  public int getSize() {
    return firstList.size();
  }

  /**
   * Check if the list is empty.
   *
   * @return True iff there are no pairs in the list
   */
  public boolean isEmpty() {
    return getSize() == 0;
  }

  /**
   * Get iterator through elements of this object.
   *
   * @return {@link Iterator} iterator
   */
  public Iterator getIterator() {
    return new Iterator();
  }

  /**
   * Special iterator class which we'll use to iterate through elements of
   * {@link PairList}, without having to create new object as wrapper for
   * each pair.
   *
   * Protocol is somewhat similar to the protocol of {@link java.util.Iterator}
   * only here next() doesn't return the next object, it just moves along in
   * the collection. Values related to current pair can be retrieved by calling
   * getCurrentFirst() and getCurrentSecond() methods.
   *
   * Not thread-safe.
   */
  public class Iterator {
    /** Current position of the iterator */
    private int position = -1;

    /**
     * Returns true if the iteration has more elements.
     *
     * @return True if the iteration has more elements.
     */
    public boolean hasNext() {
      return position < getSize() - 1;
    }

    /**
     * Moves to the next element in the iteration.
     */
    public void next() {
      position++;
    }

    /**
     * Get first element of the current pair of the iteration.
     *
     * @return First element of the current pair of the iteration
     */
    public U getCurrentFirst() {
      return firstList.get(position);
    }

    /**
     * Get second element of the current pair of the iteration.
     *
     * @return Second element of the current pair of the iteration
     */
    public V getCurrentSecond() {
      return secondList.get(position);
    }
  }
}
