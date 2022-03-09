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

import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Collects incoming edges for vertices owned by this worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public interface EdgeStore<I extends WritableComparable,
   V extends Writable, E extends Writable> {
  /**
   * Add edges belonging to a given partition on this worker.
   * Note: This method is thread-safe.
   *
   * @param partitionId Partition id for the incoming edges.
   * @param edges Incoming edges
   */
  void addPartitionEdges(int partitionId, VertexIdEdges<I, E> edges);

  /**
   * Move all edges from temporary storage to their source vertices.
   * Note: this method is not thread-safe and is called once all vertices and
   * edges are read in INPUT_SUPERSTEP.
   */
  void moveEdgesToVertices();

  /**
   * Deserialize the edges of a given partition, and removes the associated data
   * from the store.
   * Note: This method is not thread-safe (i.e. should not be called for the
   * same partition at the same time).
   *
   * @param partitionId Id of partition to deserialize
   * @param output Output to write the edge store to
   */
  void writePartitionEdgeStore(int partitionId, DataOutput output)
      throws IOException;

  /**
   * Serialize the edges of a given partition, and adds it to the partition
   * store (assumes that the edge store does not have any edge from the
   * partition already).
   * Note: This method is not thread-safe (i.e. should not be called for the
   * same partition at the same time).
   *
   * @param partitionId Id of partition to serialize
   * @param input Input to read the partition from
   */
  void readPartitionEdgeStore(int partitionId, DataInput input)
      throws IOException;

  /**
   * Check if edge store has edge for a given partition
   *
   * @param partitionId Id of partition
   * @return True iff edge store have messages for the given partition
   */
  boolean hasEdgesForPartition(int partitionId);
}
