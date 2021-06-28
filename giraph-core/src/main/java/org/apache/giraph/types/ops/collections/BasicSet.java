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
package org.apache.giraph.types.ops.collections;

import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.hadoop.io.Writable;

/**
 * BasicSet with only basic set of operations.
 * All operations that return object T are returning reusable object,
 * which is modified after calling any other function.
 *
 * @param <T> Element type
 */
public interface BasicSet<T> extends Writable {
  /** Threshold for using OpenHashSet and OpenHashBigSet implementations. */
  long MAX_OPEN_HASHSET_CAPACITY = 800000000;

  /** Removes all of the elements from this list. */
  void clear();

  /**
   * Number of elements in this list
   *
   * @return size
   */
  long size();

  /**
   * Makes sure set is not using space with capacity more than
   * max(n,size()) entries.
   *
   * @param n the threshold for the trimming.
   */
  void trim(long n);

  /**
   * Adds value to the set.
   * Returns <tt>true</tt> if set changed as a
   * result of the call.
   *
   * @param value Value to add
   * @return true if set was changed.
   */
  boolean add(T value);

  /**
   * Checks whether set contains given value
   *
   * @param value Value to check
   * @return true if value is present in the set
   */
  boolean contains(T value);

  /**
   * TypeOps for type of elements this object holds
   *
   * @return TypeOps
   */
  PrimitiveIdTypeOps<T> getElementTypeOps();
}
