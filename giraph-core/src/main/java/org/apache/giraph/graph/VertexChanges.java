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

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;

/**
 * Structure to hold all the possible graph mutations that can occur during a
 * superstep.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface VertexChanges<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * Get the added vertices for this particular vertex index from the previous
   * superstep.
   *
   * @return List of vertices for this vertex index.
   */
  List<Vertex<I, V, E>> getAddedVertexList();

  /**
   * Get the number of times this vertex was removed in the previous
   * superstep.
   *
   * @return Count of time this vertex was removed in the previous superstep
   */
  int getRemovedVertexCount();

  /**
   * Get the added edges for this particular vertex index from the previous
   * superstep
   *
   * @return List of added edges for this vertex index
   */
  List<Edge<I, E>> getAddedEdgeList();

  /**
   * Get the removed edges by their destination vertex index.
   *
   * @return List of destination edges for removal from this vertex index
   */
  List<I> getRemovedEdgeList();
}
