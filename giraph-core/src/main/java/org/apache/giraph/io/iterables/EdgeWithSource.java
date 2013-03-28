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

package org.apache.giraph.io.iterables;

import org.apache.giraph.edge.ReusableEdge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper for edge and its source id
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class EdgeWithSource<I extends WritableComparable,
    E extends Writable> {
  /** Source id */
  private I sourceVertexId;
  /** Edge */
  private ReusableEdge<I, E> edge;

  /**
   * Constructor
   */
  public EdgeWithSource() {
  }

  /**
   * Constructor with source id and edge
   *
   * @param sourceVertexId Source id
   * @param edge Edge
   */
  public EdgeWithSource(I sourceVertexId, ReusableEdge<I, E> edge) {
    this.sourceVertexId = sourceVertexId;
    this.edge = edge;
  }

  public I getSourceVertexId() {
    return sourceVertexId;
  }

  public void setSourceVertexId(I sourceVertexId) {
    this.sourceVertexId = sourceVertexId;
  }

  public ReusableEdge<I, E> getEdge() {
    return edge;
  }

  public void setEdge(ReusableEdge<I, E> edge) {
    this.edge = edge;
  }

  public I getTargetVertexId() {
    return edge.getTargetVertexId();
  }

  /**
   * Set target vertex id of this edge
   *
   * @param targetVertexId Target vertex id
   */
  public void setTargetVertexId(I targetVertexId) {
    edge.setTargetVertexId(targetVertexId);
  }

  public E getEdgeValue() {
    return edge.getValue();
  }

  /**
   * Set the value of this edge
   *
   * @param value Edge value
   */
  public void setEdgeValue(E value) {
    edge.setValue(value);
  }
}
