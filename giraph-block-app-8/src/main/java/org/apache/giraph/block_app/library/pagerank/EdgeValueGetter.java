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

import java.io.Serializable;

/**
 * Edge value getter
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public interface EdgeValueGetter<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends Serializable {
  /**
   * Get edge value from a vertex and its edge
   *
   * @param vertex Vertex
   * @param edgeValue Edge value
   * @return Edge value as double
   */
  double getEdgeValue(Vertex<I, V, E> vertex, E edgeValue);

  /**
   * Check if for one vertex all out edges have the same value
   *
   * @return Whether for one vertex all out edges have the same value
   */
  default boolean allVertexEdgesTheSame() {
    return false;
  }
}
