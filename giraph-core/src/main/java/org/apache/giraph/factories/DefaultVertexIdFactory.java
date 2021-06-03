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
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default {@link VertexIdFactory} that simply uses the default
 * constructor for the vertex ID class.
 *
 * @param <I> Vertex ID
 */
public class DefaultVertexIdFactory<I extends WritableComparable>
    implements VertexIdFactory<I>, GiraphConfigurationSettable {
  /** Cached vertex value class. */
  private Class<I> vertexIdClass;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration conf;

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
    vertexIdClass = conf.getVertexIdClass();
  }

  @Override
  public I newInstance() {
    return WritableUtils.createWritable(vertexIdClass, conf);
  }
}
