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

import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Structure that keeps partition information.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public interface PartitionData<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * Add a *new* partition to the store. If the partition is already existed,
   * it does not add the partition and returns false.
   *
   * @param partition Partition to add
   * @return Whether the addition made any change in the partition store
   */
  boolean addPartition(Partition<I, V, E> partition);

  /**
   * Remove a partition and return it. Called from a single thread, *not* from
   * within a scheduling cycle, and after INPUT_SUPERSTEP is complete.
   *
   * @param partitionId Partition id
   * @return The removed partition
   */
  Partition<I, V, E> removePartition(Integer partitionId);

  /**
   * Whether a specific partition is present in the store.
   *
   * @param partitionId Partition id
   * @return True iff the partition is present
   */
  boolean hasPartition(Integer partitionId);

  /**
   * Return the ids of all the stored partitions as an Iterable.
   *
   * @return The partition ids
   */
  Iterable<Integer> getPartitionIds();

  /**
   * Return the number of stored partitions.
   *
   * @return The number of partitions
   */
  int getNumPartitions();

  /**
   * Return the number of vertices in a partition.
   *
   * @param partitionId Partition id
   * @return The number of vertices in the specified partition
   */
  long getPartitionVertexCount(Integer partitionId);

  /**
   * Return the number of edges in a partition.
   *
   * @param partitionId Partition id
   * @return The number of edges in the specified partition
   */
  long getPartitionEdgeCount(Integer partitionId);

  /**
   * Whether the partition store is empty.
   *
   * @return True iff there are no partitions in the store
   */
  boolean isEmpty();

  /**
   * Add vertices to a given partition from a given DataOutput instance. This
   * method is called right after receipt of vertex request in INPUT_SUPERSTEP.
   *
   * @param partitionId Partition id
   * @param extendedDataOutput Output containing serialized vertex data
   */
  void addPartitionVertices(Integer partitionId,
      ExtendedDataOutput extendedDataOutput);

  /**
   * Add edges to a given partition from a given send edge request. This
   * method is called right after receipt of edge request in INPUT_SUPERSTEP.
   *
   * @param partitionId Partition id
   * @param edges Edges in the request
   */
  void addPartitionEdges(Integer partitionId, VertexIdEdges<I, E> edges);
}
