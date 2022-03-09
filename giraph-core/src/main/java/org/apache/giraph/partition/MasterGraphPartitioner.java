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

import java.util.Collection;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.giraph.worker.WorkerInfo;

/**
 * Determines how to divide the graph into partitions, how to manipulate
 * partitions and then how to assign those partitions to workers.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface MasterGraphPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * Set some initial partition owners for the graph. Guaranteed to be called
   * prior to the graph being loaded (initial or restart).
   *
   * @param availableWorkerInfos Workers available for partition assignment
   * @param maxWorkers Maximum number of workers
   * @return Collection of generated partition owners.
   */
  Collection<PartitionOwner> createInitialPartitionOwners(
      Collection<WorkerInfo> availableWorkerInfos, int maxWorkers);

  /**
   * Sets partition owners for the graph.
   * Used then loading from checkpoint.
   * @param partitionOwners assigned partition owners.
   */
  void setPartitionOwners(Collection<PartitionOwner> partitionOwners);

  /**
   * After the worker stats have been merged to a single list, the master can
   * use this information to send commands to the workers for any
   * {@link Partition} changes. This protocol is specific to the
   * {@link MasterGraphPartitioner} implementation.
   *
   * @param allPartitionStatsList All partition stats from all workers.
   * @param availableWorkers Workers available for partition assignment
   * @param maxWorkers Maximum number of workers
   * @param superstep Partition owners will be set for this superstep
   * @return Collection of {@link PartitionOwner} objects that changed from
   *         the previous superstep, empty list if no change.
   */
  Collection<PartitionOwner> generateChangedPartitionOwners(
      Collection<PartitionStats> allPartitionStatsList,
      Collection<WorkerInfo> availableWorkers,
      int maxWorkers,
      long superstep);

  /**
   * Get current partition owners at this time.
   *
   * @return Collection of current {@link PartitionOwner} objects
   */
  Collection<PartitionOwner> getCurrentPartitionOwners();

  /**
   * Instantiate the {@link PartitionStats} implementation used to read the
   * worker stats
   *
   * @return Instantiated {@link PartitionStats} object
   */
  PartitionStats createPartitionStats();
}
