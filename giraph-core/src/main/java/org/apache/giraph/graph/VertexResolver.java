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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Handles all the situations that can arise upon creation/removal of
 * vertices and edges.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public interface VertexResolver<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * A vertex may have been removed, created zero or more times and had
   * zero or more messages sent to it.  This method will handle all situations
   * excluding the normal case (a vertex already exists and has zero or more
   * messages sent it to).
   *
   * @param vertexId Vertex id (can be used for {@link Vertex}'s initialize())
   * @param vertex Original vertex or null if none
   * @param vertexChanges Changes that happened to this vertex or null if none
   * @param hasMessages True iff vertex received messages in the last superstep
   * @return Vertex to be returned, if null, and a vertex currently exists
   *         it will be removed
   */
  Vertex<I, V, E> resolve(I vertexId,
      Vertex<I, V, E> vertex,
      VertexChanges<I, V, E> vertexChanges,
      boolean hasMessages);
}
