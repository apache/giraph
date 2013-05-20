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

import com.google.common.collect.Lists;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Collection;
import java.util.List;

/**
 * A range-based worker partitioner where equal-sized ranges of vertex ids
 * are deterministically assigned to partitions.
 * The user has to define a mapping from vertex ids to long keys dense in
 * [0, keySpaceSize).
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public abstract class SimpleRangeWorkerPartitioner<I extends
    WritableComparable, V extends Writable, E extends Writable>
    implements WorkerGraphPartitioner<I, V, E> {
  /** List of {@link PartitionOwner}s for this worker. */
  private List<PartitionOwner> partitionOwnerList = Lists.newArrayList();
  /** Vertex keys space size. */
  private long keySpaceSize;

  /**
   * Constructor.
   *
   * @param keySpaceSize Vertex keys space size.
   */
  public SimpleRangeWorkerPartitioner(long keySpaceSize) {
    this.keySpaceSize = keySpaceSize;
  }

  /**
   * Get key space size (can be used when implementing vertexKeyFromId()).
   *
   * @return Key space size.
   */
  public long getKeySpaceSize() {
    return keySpaceSize;
  }

  /**
   * Convert a vertex id to a unique long key in [0, keySpaceSize].
   *
   * @param id Vertex id
   * @return Unique long key
   */
  protected abstract long vertexKeyFromId(I id);

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
    long rangeSize = keySpaceSize / partitionOwnerList.size();
    return partitionOwnerList.get(
        Math.min((int) (vertexKeyFromId(vertexId) / rangeSize),
            partitionOwnerList.size() - 1));
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<I, V, E> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(
      WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners,
      PartitionStore<I, V, E> partitionStore) {
    return PartitionBalancer.updatePartitionOwners(partitionOwnerList,
        myWorkerInfo, masterSetPartitionOwners, partitionStore);
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return partitionOwnerList;
  }
}
