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
import org.apache.log4j.Logger;

/**
 * This vertex should only be used in conjunction with ByteArrayPartition since
 * it has special code to deserialize by reusing objects and not instantiating
 * new ones.  If used without ByteArrayPartition, it will cause a lot of
 * wasted memory.
 *
 * Also, this vertex is optimized for space and not efficient for either adding
 * or random access of edges.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class RepresentativeVertex<
    I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends RepresentativeVertexBase<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RepresentativeVertex.class);

  @Override
  public final boolean addEdge(Edge<I, E> edge) {
    // Note that this is very expensive (deserializes all edges
    // in an addEdge() request).
    // Hopefully the user set all the edges in setEdges().
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
  public final int removeEdges(I targetVertexId) {
    return removeFirstEdge(targetVertexId) ? 1 : 0;
  }
}

