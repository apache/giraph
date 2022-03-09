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

package org.apache.giraph.utils;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Special iterator that reuses vertex ids and edge objects so that the
 * lifetime of the object is only until next() is called.
 *
 * @param <I> vertexId type parameter
 * @param <E> edge type parameter
 */
public interface VertexIdEdgeIterator<I extends WritableComparable,
    E extends Writable> extends VertexIdDataIterator<I, Edge<I, E>> {
  /**
   * Get the current edge.
   *
   * @return Current edge
   */
  Edge<I, E> getCurrentEdge();

  /**
   * Release the current edge.
   *
   * @return Released edge
   */
  Edge<I, E> releaseCurrentEdge();
}
