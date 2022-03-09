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

/**
 * Simple immutable structure for storing a final vertex and edge count.
 */
public class VertexEdgeCount {
  /** Immutable vertices */
  private final long vertexCount;
  /** Immutable edges */
  private final long edgeCount;
  /** Immutable mappings */
  private final long mappingCount;

  /**
   * Default constructor.
   */
  public VertexEdgeCount() {
    vertexCount = 0;
    edgeCount = 0;
    mappingCount = 0;
  }

  /**
   * Constructor with initial values.
   *
   * @param vertexCount Final number of vertices.
   * @param edgeCount Final number of edges.
   * @param mappingCount Final number of mappings.
   */
  public VertexEdgeCount(long vertexCount, long edgeCount, long mappingCount) {
    this.vertexCount = vertexCount;
    this.edgeCount = edgeCount;
    this.mappingCount = mappingCount;
  }

  public long getVertexCount() {
    return vertexCount;
  }

  public long getEdgeCount() {
    return edgeCount;
  }

  public long getMappingCount() {
    return mappingCount;
  }

  /**
   * Increment the both the vertex edge count with a {@link VertexEdgeCount}.
   *
   * @param vertexEdgeCount add both the vertices and edges of this object.
   * @return New immutable object with the new vertex and edge counts.
   */
  public VertexEdgeCount incrVertexEdgeCount(
      VertexEdgeCount vertexEdgeCount) {
    return new VertexEdgeCount(
        vertexCount + vertexEdgeCount.getVertexCount(),
        edgeCount + vertexEdgeCount.getEdgeCount(),
        mappingCount + vertexEdgeCount.getMappingCount());
  }

  /**
   * Increment the both the vertex edge count with primitives.
   *
   * @param vertexCount Add this many vertices.
   * @param edgeCount Add this many edges.
   * @return New immutable object with the new vertex and edge counts.
   */
  public VertexEdgeCount incrVertexEdgeCount(
      long vertexCount, long edgeCount) {
    return new VertexEdgeCount(
        this.vertexCount + vertexCount,
        this.edgeCount + edgeCount,
        this.mappingCount + mappingCount);
  }

  @Override
  public String toString() {
    return "(v=" + getVertexCount() + ", e=" + getEdgeCount() +
        (mappingCount > 0 ? ", m=" + mappingCount : "") + ")";
  }
}
