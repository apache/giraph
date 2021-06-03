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

package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Edge value getter where all vertex edges have the same edge value
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public interface UnweightedEdgeValueGetter<I extends WritableComparable,
    V extends Writable, E extends Writable> extends EdgeValueGetter<I, V, E> {
  /**
   * Get edge value for out edges from a vertex
   *
   * @param vertex Vertex
   * @return Edge value as double
   */
  double getEdgeValue(Vertex<I, V, E> vertex);

  @Override
  default double getEdgeValue(Vertex<I, V, E> vertex, E edgeValue) {
    return getEdgeValue(vertex);
  }

  @Override
  default boolean allVertexEdgesTheSame() {
    return true;
  }
}
