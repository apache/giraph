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

package org.apache.giraph.edge;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for {@link OutEdges} implementations that provide efficient
 * random access to the edges given the target vertex id.
 * This version is for strict graphs (i.e. assumes no parallel edges).
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public interface StrictRandomAccessOutEdges<I extends WritableComparable,
    E extends Writable> extends OutEdges<I, E> {
  /**
   * Return the edge value for the given target vertex id (or null if there
   * is no edge pointing to it).
   *
   * @param targetVertexId Target vertex id
   * @return Edge value
   */
  E getEdgeValue(I targetVertexId);

  /**
   * Set the edge value for the given target vertex id (if an edge to that
   * vertex exists).
   *
   * @param targetVertexId Target vertex id
   * @param edgeValue Edge value
   */
  void setEdgeValue(I targetVertexId, E edgeValue);
}
