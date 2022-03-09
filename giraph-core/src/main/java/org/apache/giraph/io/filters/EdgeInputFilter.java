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
package org.apache.giraph.io.filters;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Filters edges on input.
 *
 * @param <I> Vertex ID
 * @param <E> Edge Value
 */
public interface EdgeInputFilter<I extends WritableComparable,
    E extends Writable> {
  /**
   * Whether to drop this edge
   *
   * @param sourceId ID of source of edge
   * @param edge to check
   * @return true if we should drop the edge
   */
  boolean dropEdge(I sourceId, Edge<I, E> edge);
}

