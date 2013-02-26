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

package org.apache.giraph.io;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Vertex reader for {@link org.apache.giraph.io.VertexValueInputFormat}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
public abstract class VertexValueReader<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BasicVertexValueReader<I, V, E, M> {
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    configuration = new ImmutableClassesGiraphConfiguration<I, V, E, M>(
        context.getConfiguration());
  }

  @Override
  public final Vertex<I, V, E, M> getCurrentVertex() throws IOException,
      InterruptedException {
    Vertex<I, V, E, M> vertex = getConf().createVertex();
    vertex.initialize(getCurrentVertexId(), getCurrentVertexValue());
    return vertex;
  }

  public ImmutableClassesGiraphConfiguration<I, V, E, M> getConf() {
    return configuration;
  }
}
