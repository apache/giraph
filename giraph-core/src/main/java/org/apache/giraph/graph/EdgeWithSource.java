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

/**
 * A pair of source vertex id and Edge object (that is,
 * all the information about an edge).
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class EdgeWithSource<I extends WritableComparable, E extends Writable> {
  /** Source vertex id. */
  private final I sourceVertexId;
  /** Edge. */
  private final Edge<I, E> edge;

  /**
   * Constructor.
   *
   * @param sourceVertexId Source vertex id
   * @param edge Edge
   */
  public EdgeWithSource(I sourceVertexId, Edge<I, E> edge) {
    this.sourceVertexId = sourceVertexId;
    this.edge = edge;
  }

  /**
   * Get the source vertex id.
   *
   * @return Source vertex id.
   */
  public I getSourceVertexId() {
    return sourceVertexId;
  }

  /**
   * Get the edge object.
   *
   * @return The edge.
   */
  public Edge<I, E> getEdge() {
    return edge;
  }
}
