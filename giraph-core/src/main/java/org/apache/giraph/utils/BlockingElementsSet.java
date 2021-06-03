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

import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Keeps a set of elements, and allows for waiting on certain number of
 * elements to become available. Assumes that at any point no more elements
 * than we'll be asking for will be added to the set. Reusable.
 *
 * @param <T> Element type
 */
public class BlockingElementsSet<T> {
  /** Semaphore to keep track of element count */
  private final Semaphore semaphore = new Semaphore(0);
  /** Elements */
  private final List<T> elements =
      Collections.synchronizedList(new ArrayList<T>());

  /**
   * Put an element in the set
   *
   * @param element Element to put
   */
  public void offer(T element) {
    elements.add(element);
    semaphore.release();
  }

  /**
   * Get one element when it becomes available,
   * reporting progress while waiting
   *
   * @param progressable Progressable to report progress
   * @return Element acquired
   */
  public T getElement(Progressable progressable) {
    return getElements(1, progressable).get(0);
  }

  /**
   * Get desired number of elements when they become available,
   * reporting progress while waiting
   *
   * @param elementCount How many elements to wait for
   * @param progressable Progressable to report progress
   * @return List of elements acquired
   */
  public List<T> getElements(int elementCount, Progressable progressable) {
    ProgressableUtils.awaitSemaphorePermits(
        semaphore, elementCount, progressable);
    Preconditions.checkState(elements.size() == elementCount);
    List<T> ret = new ArrayList<>(elements);
    elements.clear();
    return ret;
  }
}
