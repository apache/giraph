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

package org.apache.giraph.graph;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Default {@link VertexValueFactory} that simply uses the default
 * constructor for the vertex value class.
 *
 * @param <V> Vertex value
 */
public class DefaultVertexValueFactory<V extends Writable>
    implements VertexValueFactory<V> {
  /** Cached vertex value class. */
  private Class<V> vertexValueClass;

  @Override
  public void initialize(
      ImmutableClassesGiraphConfiguration<?, V, ?, ?> configuration) {
    vertexValueClass = configuration.getVertexValueClass();
  }

  @Override
  public V createVertexValue() {
    if (vertexValueClass == NullWritable.class) {
      return (V) NullWritable.get();
    } else {
      try {
        return vertexValueClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(
            "createVertexValue: Failed to instantiate", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(
            "createVertexValue: Illegally accessed", e);
      }
    }
  }
}
