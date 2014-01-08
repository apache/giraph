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
import java.util.List;

import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

/**
 * Abstracts and implements all WorkerGraphPartitioner logic on top of a single
 * user function - getPartitionIndex.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public abstract class SimpleWorkerPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements WorkerGraphPartitioner<I, V, E> {
  /** List of {@link PartitionOwner}s for this worker. */
  private List<PartitionOwner> partitionOwnerList = Lists.newArrayList();

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
    return partitionOwnerList.get(
        getPartitionIndex(vertexId, partitionOwnerList.size()));
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<I, V, E> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners,
      PartitionStore<I, V, E> partitionStore) {
    return PartitionBalancer.updatePartitionOwners(partitionOwnerList,
        myWorkerInfo, masterSetPartitionOwners, partitionStore);
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return partitionOwnerList;
  }

  /**
   * Calculates in which partition current vertex belongs to,
   * from interval [0, partitionCount).
   *
   * @param id Vertex id
   * @param partitionCount Number of partitions
   * @return partition
   */
  protected abstract int getPartitionIndex(I id, int partitionCount);
}
