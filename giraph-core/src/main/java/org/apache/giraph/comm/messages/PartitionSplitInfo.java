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
package org.apache.giraph.comm.messages;

import org.apache.giraph.partition.Partition;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface providing partition split information.
 *
 * @param <I> Vertex id type.
 */
public interface PartitionSplitInfo<I extends WritableComparable> {
  /**
   * Get the partition id that a vertex id would belong to.
   *
   * @param vertexId Vertex id
   * @return Partition id
   */
  int getPartitionId(I vertexId);

  /**
   * Get the ids of all the stored partitions (on current worker) as Iterable
   *
   * @return The partition ids
   */
  Iterable<Integer> getPartitionIds();

  /**
   * Return the number of vertices in a partition.
   *
   * @param partitionId Partition id
   * @return The number of vertices in the specified partition
   */
  long getPartitionVertexCount(Integer partitionId);

  /**
   * {@link org.apache.giraph.partition.PartitionStore#startIteration()}
   */
  void startIteration();

  /**
   * {@link org.apache.giraph.partition.PartitionStore#getNextPartition()}
   *
   * @return The next partition to process
   */
  Partition getNextPartition();

  /**
   * {@link org.apache.giraph.partition.PartitionStore#putPartition(Partition)}
   *
   * @param partition Partition
   */
  void putPartition(Partition partition);
}
