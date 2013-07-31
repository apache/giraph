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
package org.apache.giraph.factories;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;

/**
 * Base interface for factories creating user graph value types (IVEMM)
 *
 * @param <W> Writable type
 */
public interface ValueFactory<W extends Writable> {
  /**
   * Initialize factory settings from the conf.
   * This gets called on startup and also if there are changes to the message
   * classes used. For example if the user's
   * {@link org.apache.giraph.master.MasterCompute} changes the
   * {@link org.apache.giraph.graph.Computation} and the next superstep has a
   * different message value type.
   *
   * @param conf Configuration
   */
  void initialize(ImmutableClassesGiraphConfiguration conf);

  /**
   * Create a new value.
   *
   * @return new value.
   */
  W newInstance();

  /**
   * Get the java Class representing messages this factory creates
   *
   * @return Class<M>
   */
  Class<W> getValueClass();
}
