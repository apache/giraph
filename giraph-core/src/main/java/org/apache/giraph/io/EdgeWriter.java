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

import java.io.IOException;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class EdgeWriter<
    I extends WritableComparable, V extends Writable,
    E extends Writable> extends
    DefaultImmutableClassesGiraphConfigurable<I, V, E> {

  /**
   * Writes the next vertex and associated data
   *
   * @param   sourceId    the vertex ID from which the edge originates
   * @param   sourceValue the vertex value; the vertex is the one from which
   *                      the edge originates
   * @param   edge        edge to be written
   * @throws  IOException
   * @throws  InterruptedException
   */
  public abstract void writeEdge(I sourceId, V sourceValue, Edge<I, E> edge)
    throws IOException, InterruptedException;

  /**
   * Use the context to setup writing the edges.
   * Guaranteed to be called prior to any other function.
   *
   * @param  context Context used to write the vertices.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void initialize(TaskAttemptContext context)
    throws IOException, InterruptedException;

  /**
   * Close this {@link EdgeWriter} to future operations.
   *
   * @param  context the context of the task
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void close(TaskAttemptContext context)
    throws IOException, InterruptedException;
}
