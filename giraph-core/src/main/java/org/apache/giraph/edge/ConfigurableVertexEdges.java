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

package org.apache.giraph.edge;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract base class for {@link VertexEdges} implementations that require
 * access to the configuration.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
@SuppressWarnings("unchecked")
public abstract class ConfigurableVertexEdges<I extends WritableComparable,
    E extends Writable>
    implements VertexEdges<I, E>, ImmutableClassesGiraphConfigurable {
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<I, ?, E, ?> configuration;

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    configuration = conf;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, ?, E, ?> getConf() {
    return configuration;
  }
}
