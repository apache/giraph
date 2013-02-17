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
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * User applications can subclass {@link EdgeListVertex}, which stores
 * the outbound edges in an ArrayList (less memory as the cost of expensive
 * random-access lookup).  Good for static graphs.  Not nearly as memory
 * efficient as using ByteArrayVertex + ByteArrayPartition
 * (probably about 10x more), but not bad when keeping vertices as objects in
 * memory (SimplePartition).
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class EdgeListVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends EdgeListVertexBase<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(EdgeListVertex.class);
  @Override
  public final boolean addEdge(Edge<I, E> edge) {
    for (Edge<I, E> currentEdge : getEdges()) {
      if (currentEdge.getTargetVertexId().equals(edge.getTargetVertexId())) {
        LOG.warn("addEdge: Vertex=" + getId() +
            ": already added an edge value for target vertex id " +
            edge.getTargetVertexId());
        return false;
      }
    }
    appendEdge(edge);
    return true;
  }

  @Override
  public int removeEdges(I targetVertexId) {
    for (Iterator<Edge<I, E>> edges = getEdges().iterator(); edges.hasNext();) {
      Edge<I, E> edge = edges.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        edges.remove();
        return 1;
      }
    }
    return 0;
  }
}

