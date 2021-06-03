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

package org.apache.giraph.mapping;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * MappingStore used to store the vertexId - target map supplied by user
 * Methods implemented in this class need to be thread safe
 *
 * @param <I> vertexId type
 * @param <B> mappingTarget type
 */
public interface MappingStore<I extends WritableComparable, B extends Writable>
  extends ImmutableClassesGiraphConfigurable<I, Writable, Writable> {

  /**
   * Must be called before anything else can be done
   * on this instance
   */
  void initialize();

  /**
   * Add an entry to the mapping store
   *
   * @param vertexId vertexId
   * @param target target
   */
  void addEntry(I vertexId, B target);

  /**
   * Get target for given vertexId
   *
   * @param vertexId vertexId
   * @param target instance to use for storing target information
   * @return target instance
   */
  B getTarget(I vertexId, B target);

  /**
   * Operations to perform after adding entries
   * to the mapping store
   */
  void postFilling();

  /**
   * Get stats about the MappingStore
   *
   * @return numEntries
   */
  long getStats();
}
