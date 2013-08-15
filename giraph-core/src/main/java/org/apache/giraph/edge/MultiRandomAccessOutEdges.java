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
 * This version is for multigraphs (i.e. there can be parallel edges).
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public interface MultiRandomAccessOutEdges<I extends WritableComparable,
    E extends Writable> extends OutEdges<I, E> {
  /**
   * Return an iterable over the edge values for a given target vertex id.
   *
   * @param targetVertexId Target vertex id
   * @return Iterable of edge values
   */
  Iterable<E> getAllEdgeValues(I targetVertexId);
}
