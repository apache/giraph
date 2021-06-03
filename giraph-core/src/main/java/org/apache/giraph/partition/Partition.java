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

package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.VertexIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

/**
 * A generic container that stores vertices.  Vertex ids will map to exactly
 * one partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public interface Partition<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends Writable, ImmutableClassesGiraphConfigurable<I, V, E>,
    Iterable<Vertex<I, V, E>> {
  /**
   * Initialize the partition.  Guaranteed to be called before used.
   *
   * @param partitionId Partition id
   * @param progressable Progressable to call progress
   */
  void initialize(int partitionId, Progressable progressable);

  /**
   * Get the vertex for this vertex index.
   *
   * @param vertexIndex Vertex index to search for
   * @return Vertex if it exists, null otherwise
   */
  Vertex<I, V, E> getVertex(I vertexIndex);

  /**
   * Put a vertex into the Partition
   *
   * @param vertex Vertex to put in the Partition
   * @return old vertex value (i.e. null if none existed prior)
   */
  Vertex<I, V, E> putVertex(Vertex<I, V, E> vertex);

  /**
   * Remove a vertex from the Partition
   *
   * @param vertexIndex Vertex index to remove
   * @return The removed vertex.
   */
  Vertex<I, V, E> removeVertex(I vertexIndex);

  /**
   * Add a partition's vertices.  If a vertex to be added doesn't exist,
   * add it.  If the vertex already exists, use the
   * VertexValueCombiner to combine them.
   *
   * @param partition Partition to add
   */
  void addPartition(Partition<I, V, E> partition);

  /**
   * Put this vertex or combine it
   *
   * @param vertex Vertex to put or combine
   * @return True if the vertex was put (hint to release object)
   */
  boolean putOrCombine(Vertex<I, V, E> vertex);

  /**
   * Add vertices to a partition.  If a vertex to be added doesn't exist,
   * add it.  If the vertex already exists, use the
   * VertexValueCombiner to combine them.
   *
   * @param vertexIterator Vertices to add
   */
  void addPartitionVertices(VertexIterator<I, V, E> vertexIterator);

  /**
   * Get the number of vertices in this partition
   *
   * @return Number of vertices
   */
  long getVertexCount();

  /**
   * Get the number of edges in this partition.
   *
   * @return Number of edges.
   */
  long getEdgeCount();

  /**
   * Get the partition id.
   *
   * @return Id of this partition.
   */
  int getId();

  /**
   * Set the partition id.
   *
   * @param id Id of this partition
   */
  void setId(int id);

  /**
   * Report progress.
   */
  void progress();

  /**
   * Set the context.
   *
   * @param progressable Progressable
   */
  void setProgressable(Progressable progressable);

  /**
   * Save potentially modified vertex back to the partition.
   *
   * @param vertex Vertex to save
   */
  void saveVertex(Vertex<I, V, E> vertex);
}
