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

import java.util.Iterator;

/**
 * An edge-list backed vertex that allows for parallel edges.
 * This can be used not only to support mutable multigraphs,
 * but also to make mutations and edge-based input efficient without
 * resorting to a hash-map backed vertex.
 *
 * Note: removeEdge() here removes all edges pointing to the target vertex,
 * but returns only one of them (or null if there are no such edges).
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class MultiGraphEdgeListVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends EdgeListVertexBase<I, V, E, M> {
  @Override
  public final boolean addEdge(Edge<I, E> edge) {
    appendEdge(edge);
    return true;
  }

  @Override
  public int removeEdges(I targetVertexId) {
    int removedCount = 0;
    for (Iterator<Edge<I, E>> edges = getEdges().iterator(); edges.hasNext();) {
      Edge<I, E> edge = edges.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        ++removedCount;
        edges.remove();
      }
    }
    return removedCount;
  }
}
