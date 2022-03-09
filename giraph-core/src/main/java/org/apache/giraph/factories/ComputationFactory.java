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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Factory for creating Computations
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 * @param <M1> Incoming Message Value
 * @param <M2> Outgoing Message Value
 */
public interface ComputationFactory<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable> {
  /**
   * One time initialization before compute calls.
   * Guaranteed to be called from only one thread before computation begins.
   *
   * @param conf Configuration
   */
  void initialize(ImmutableClassesGiraphConfiguration<I, V, E> conf);

  /**
   * Get Computation object
   *
   * @param conf Configuration
   * @return Computation
   */
  Computation<I, V, E, M1, M2> createComputation(
      ImmutableClassesGiraphConfiguration<I, V, E> conf);

  /**
   * Check that the Configuration passed in is setup correctly to run a job.
   *
   * @param conf Configuration to check.
   */
  void checkConfiguration(
      ImmutableClassesGiraphConfiguration<I, V, E> conf);

  /**
   * Get name of this particular computation
   *
   * @param conf Configuration
   * @return String name of computation
   */
  String computationName(GiraphConfiguration conf);
}
