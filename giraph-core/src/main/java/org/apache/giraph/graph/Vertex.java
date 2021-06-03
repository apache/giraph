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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class which holds vertex id, data and edges.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public interface Vertex<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    ImmutableClassesGiraphConfigurable<I, V, E> {
  /**
   * Initialize id, value, and edges.
   * This method (or the alternative form initialize(id, value)) must be called
   * after instantiation, unless readFields() is called.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Iterable of edges
   */
  void initialize(I id, V value, Iterable<Edge<I, E>> edges);

  /**
   * Initialize id and value. Vertex edges will be empty.
   * This method (or the alternative form initialize(id, value, edges))
   * must be called after instantiation, unless readFields() is called.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  void initialize(I id, V value);

  /**
   * Get the vertex id.
   *
   * @return My vertex id.
   */
  I getId();

  /**
   * Get the vertex value (data stored with vertex)
   *
   * @return Vertex value
   */
  V getValue();

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param value Vertex data to be set
   */
  void setValue(V value);

  /**
   * After this is called, the compute() code will no longer be called for
   * this vertex unless a message is sent to it.  Then the compute() code
   * will be called once again until this function is called.  The
   * application finishes only when all vertices vote to halt.
   */
  void voteToHalt();

  /**
   * Get the number of outgoing edges on this vertex.
   *
   * @return the total number of outbound edges from this vertex
   */
  int getNumEdges();

  /**
   * Get a read-only view of the out-edges of this vertex.
   * Note: edge objects returned by this iterable may be invalidated as soon
   * as the next element is requested. Thus, keeping a reference to an edge
   * almost always leads to undesired behavior.
   * Accessing the edges with other methods (e.g., addEdge()) during iteration
   * leads to undefined behavior.
   *
   * @return the out edges (sort order determined by subclass implementation).
   */
  Iterable<Edge<I, E>> getEdges();

  /**
   * Set the outgoing edges for this vertex.
   *
   * @param edges Iterable of edges
   */
  void setEdges(Iterable<Edge<I, E>> edges);

  /**
   * Get an iterable of out-edges that can be modified in-place.
   * This can mean changing the current edge value or removing the current edge
   * (by using the iterator version).
   * Note: accessing the edges with other methods (e.g., addEdge()) during
   * iteration leads to undefined behavior.
   *
   * @return An iterable of mutable out-edges
   */
  Iterable<MutableEdge<I, E>> getMutableEdges();

  /**
   * Return the value of the first edge with the given target vertex id,
   * or null if there is no such edge.
   * Note: edge value objects returned by this method may be invalidated by
   * the next call. Thus, keeping a reference to an edge value almost always
   * leads to undesired behavior.
   *
   * @param targetVertexId Target vertex id
   * @return Edge value (or null if missing)
   */
  E getEdgeValue(I targetVertexId);

  /**
   * If an edge to the target vertex exists, set it to the given edge value.
   * This only makes sense with strict graphs.
   *
   * @param targetVertexId Target vertex id
   * @param edgeValue Edge value
   */
  void setEdgeValue(I targetVertexId, E edgeValue);

  /**
   * Get an iterable over the values of all edges with the given target
   * vertex id. This only makes sense for multigraphs (i.e. graphs with
   * parallel edges).
   * Note: edge value objects returned by this method may be invalidated as
   * soon as the next element is requested. Thus, keeping a reference to an
   * edge value almost always leads to undesired behavior.
   *
   * @param targetVertexId Target vertex id
   * @return Iterable of edge values
   */
  Iterable<E> getAllEdgeValues(final I targetVertexId);

  /**
   * Add an edge for this vertex (happens immediately)
   *
   * @param edge Edge to add
   */
  void addEdge(Edge<I, E> edge);

  /**
   * Removes all edges pointing to the given vertex id.
   *
   * @param targetVertexId the target vertex id
   */
  void removeEdges(I targetVertexId);

  /**
   * If a {@link org.apache.giraph.edge.MutableEdgesWrapper} was used to
   * provide a mutable iterator, copy any remaining edges to the new
   * {@link org.apache.giraph.edge.OutEdges} data structure and keep a direct
   * reference to it (thus discarding the wrapper).
   * Called by the Giraph infrastructure after computation.
   */
  void unwrapMutableEdges();

  /**
   * Re-activate vertex if halted.
   */
  void wakeUp();

  /**
   * Is this vertex done?
   *
   * @return True if halted, false otherwise.
   */
  boolean isHalted();
}

