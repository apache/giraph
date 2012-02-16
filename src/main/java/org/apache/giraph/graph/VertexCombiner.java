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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract class to extend for combining of messages sent to the same vertex.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public abstract class VertexCombiner<I extends WritableComparable,
    M extends Writable> {
  /**
   * Combines message values for a particular vertex index.
   *
   * @param vertexIndex Index of the vertex getting these messages
   * @param messages Iterable of the messages to be combined
   * @return Iterable of the combined messages. The returned value cannot
   *         be null and its size is required to be smaller or equal to
   *         the size of the messages list.
   * @throws IOException
   */
  public abstract Iterable<M> combine(I vertexIndex,
      Iterable<M> messages) throws IOException;
}
