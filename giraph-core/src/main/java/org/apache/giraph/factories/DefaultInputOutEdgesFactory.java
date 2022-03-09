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

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default factory class for creating {@link OutEdges} instances to be used
 * during input. This factory simply creates an instance of the
 * {@link OutEdges} class set in the configuration.
 *
 * @param <I> Vertex ID type.
 * @param <E> Edge value type.
 */
public class DefaultInputOutEdgesFactory<I extends WritableComparable,
  E extends Writable> implements OutEdgesFactory<I, E>,
  GiraphConfigurationSettable {
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, E> conf;

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public OutEdges<I, E> newInstance() {
    return ReflectionUtils.newInstance(conf.getInputOutEdgesClass(), conf);
  }
}
