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

package org.apache.giraph.io.iterables;

import java.util.Iterator;

/**
 * Wraps {@link Iterator} into object which provides iteration like in
 * {@link org.apache.giraph.io.VertexReader} or
 * {@link org.apache.giraph.io.EdgeReader}
 *
 * @param <T>
 */
public class IteratorToReaderWrapper<T> {
  /** Wrapped iterator */
  private Iterator<T> iterator;
  /** Current object */
  private T currentObject = null;

  /**
   * Constructor
   *
   * @param iterator Iterator to wrap
   */
  public IteratorToReaderWrapper(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  /**
   * Read next object
   *
   * @return False iff there are no more objects
   */
  public boolean nextObject() {
    boolean hasNext = iterator.hasNext();
    if (hasNext) {
      currentObject = iterator.next();
    } else {
      currentObject = null;
    }
    return hasNext;
  }

  /**
   * Get the current object
   *
   * @return Current object
   */
  public T getCurrentObject() {
    return currentObject;
  }
}
