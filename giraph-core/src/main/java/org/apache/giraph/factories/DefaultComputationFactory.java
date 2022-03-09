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
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default computation factory that simply creates java computation object
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class DefaultComputationFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements ComputationFactory<I, V, E, Writable, Writable> {
  @Override
  public void initialize(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    // Nothing to do here
  }

  @Override
  public Computation<I, V, E, Writable, Writable> createComputation(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    Class<? extends Computation> klass = conf.getComputationClass();
    return ReflectionUtils.newInstance(klass, conf);
  }

  @Override
  public void checkConfiguration(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    if (conf.getComputationClass() == null) {
      throw new IllegalArgumentException("checkConfiguration: Null " +
          GiraphConstants.COMPUTATION_CLASS.getKey());
    }
  }

  @Override
  public String computationName(GiraphConfiguration conf) {
    return conf.getComputationClass().getSimpleName();
  }
}
