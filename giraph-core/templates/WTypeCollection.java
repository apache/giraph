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

import org.apache.giraph.function.primitive.${type.camel}Consumer;
import org.apache.giraph.function.primitive.${type.camel}Predicate;
import org.apache.hadoop.io.${type.camel}Writable;

import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Collection;

${generated_message}

/**
 * Long specialization of WCollection
 */
public interface W${type.camel}Collection
    extends WCollection<${type.camel}Writable>, ${type.camel}Collection {
  /**
   * Traverse all elements of the array list, calling given function on each
   * element, or until predicate returns false.
   *
   * @param f Function to call on each element.
   */
  void forEach${type.camel}(${type.camel}Consumer f);

  /**
   * Traverse all elements of the array list, calling given function on each
   * element.
   *
   * @param f Function to call on each element.
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   */
  boolean forEachWhile${type.camel}(${type.camel}Predicate f);
}
