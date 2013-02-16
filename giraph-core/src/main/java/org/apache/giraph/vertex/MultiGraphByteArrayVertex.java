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

package org.apache.giraph.vertex;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Similar to {@link ByteArrayVertex}, but allows for parallel edges.
 *
 * Note:  removeEdge() here removes all edges pointing to the target vertex,
 * but returns only one of them (or null if there are no such edges).
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class MultiGraphByteArrayVertex<I extends
    WritableComparable, V extends Writable, E extends Writable,
    M extends Writable> extends ByteArrayVertexBase<I, V, E, M> {
  @Override
  public final boolean addEdge(Edge<I, E> edge) {
    appendEdge(edge);
    return true;
  }

  @Override
  public final int removeEdges(I targetVertexId) {
    return removeAllEdges(targetVertexId);
  }
}
