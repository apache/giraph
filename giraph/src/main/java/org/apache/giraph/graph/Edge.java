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
 * A complete edge, the target vertex and the edge value.  Can only be one
 * edge with a destination vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class Edge<I extends WritableComparable, E extends Writable>
    implements Comparable<Edge<I, E>> {
  /** Target vertex id */
  private I targetVertexId = null;
  /** Edge value */
  private E value = null;

  /**
   * Constructor for reflection
   */
  public Edge() { }

  /**
   * Create the edge with final values
   *
   * @param targetVertexId Desination vertex id.
   * @param value Value of the edge.
   */
  public Edge(I targetVertexId, E value) {
    this.targetVertexId = targetVertexId;
    this.value = value;
  }

  /**
   * Get the target vertex index of this edge
   *
   * @return Target vertex index of this edge
   */
  public I getTargetVertexId() {
    return targetVertexId;
  }

  /**
   * Get the edge value of the edge
   *
   * @return Edge value of this edge
   */
  public E getValue() {
    return value;
  }

  /**
   * Set the destination vertex index of this edge.
   *
   * @param targetVertexId new destination vertex
   */
  public void setTargetVertexId(I targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  /**
   * Set the value for this edge.
   *
   * @param value new edge value
   */
  public void setValue(E value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "(TargetVertexId = " + targetVertexId + ", " +
        "value = " + value + ")";
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Edge<I, E> edge) {
    return targetVertexId.compareTo(edge.getTargetVertexId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Edge edge = (Edge) o;

    if (targetVertexId != null ? !targetVertexId.equals(edge.targetVertexId) :
      edge.targetVertexId != null) {
      return false;
    }
    if (value != null ? !value.equals(edge.value) : edge.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = targetVertexId != null ? targetVertexId.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
