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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Iterates over vertices stored in an ExtendedDataOutput such that
 * the ownership of the vertex id can be transferred to another object.
 * This optimization cuts down on the number of objects instantiated and
 * garbage collected
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class VertexIterator<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Reader of the serialized edges */
  private final ExtendedDataInput extendedDataInput;
  /** Current vertex */
  private Vertex<I, V, E> vertex;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> configuration;

  /**
   * Constructor.
   *
   * @param extendedDataOutput Extended data output
   * @param configuration Configuration
   */
  public VertexIterator(
      ExtendedDataOutput extendedDataOutput,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration) {
    extendedDataInput = configuration.createExtendedDataInput(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
    this.configuration = configuration;
    resetEmptyVertex();
  }

  /**
   * Reset the empty Vertex to an initial state.
   */
  private void resetEmptyVertex() {
    vertex = configuration.createVertex();
    I id = configuration.createVertexId();
    V value = configuration.createVertexValue();
    OutEdges<I, E> edges = configuration.createOutEdges();
    vertex.initialize(id, value, edges);
  }

  /**
   * Returns true if the iteration has more elements.
   *
   * @return True if the iteration has more elements.
   */
  public boolean hasNext() {
    return !extendedDataInput.endOfInput();
  }

  /**
   * Moves to the next element in the iteration.
   */
  public void next() {
    // If the vertex was released, create another one
    if (vertex == null) {
      resetEmptyVertex();
    }

    // If the vertex id was released, create another one
    if (vertex.getId() == null) {
      vertex.initialize(configuration.createVertexId(), vertex.getValue());
    }

    try {
      WritableUtils.reinitializeVertexFromDataInput(
          extendedDataInput, vertex, configuration);
    } catch (IOException e) {
      throw new IllegalStateException("next: IOException", e);
    }
  }

  /**
   * Get the current vertex id.  Ihis object's contents are only guaranteed
   * until next() is called.  To take ownership of this object call
   * releaseCurrentVertexId() after getting a reference to this object.
   *
   * @return Current vertex id
   */
  public I getCurrentVertexId() {
    return vertex.getId();
  }

  /**
   * The backing store of the current vertex id is now released.
   * Further calls to getCurrentVertexId () without calling next()
   * will return null.
   *
   * @return Current vertex id that was released
   */
  public I releaseCurrentVertexId() {
    I releasedVertexId = vertex.getId();
    vertex.initialize(null, vertex.getValue());
    return releasedVertexId;
  }

  public Vertex<I, V, E> getVertex() {
    return vertex;
  }

  /**
   * Release the ownership of the Vertex object to the caller
   *
   * @return Released Vertex object
   */
  public Vertex<I, V, E> releaseVertex() {
    Vertex<I, V, E> releasedVertex = vertex;
    vertex = null;
    return releasedVertex;
  }
}
