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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

/**
 * Abstracts and implements all MasterGraphPartitioner logic on top of a single
 * user function - getWorkerIndex.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public abstract class MasterGraphPartitionerImpl<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements MasterGraphPartitioner<I, V, E> {
  /** Provided configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Save the last generated partition owner list */
  private List<PartitionOwner> partitionOwnerList;

  /**
   * Constructor.
   *
   * @param conf
   *          Configuration used.
   */
  public MasterGraphPartitionerImpl(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    this.conf = conf;
  }

  @Override
  public Collection<PartitionOwner> createInitialPartitionOwners(
      Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
    int partitionCount = PartitionUtils.computePartitionCount(
        availableWorkerInfos.size(), conf);
    ArrayList<WorkerInfo> workerList =
        new ArrayList<WorkerInfo>(availableWorkerInfos);

    partitionOwnerList = new ArrayList<PartitionOwner>();
    for (int i = 0; i < partitionCount; i++) {
      partitionOwnerList.add(new BasicPartitionOwner(i, workerList.get(
          getWorkerIndex(i, partitionCount, workerList.size()))));
    }

    return partitionOwnerList;
  }

  @Override
  public void setPartitionOwners(Collection<PartitionOwner> partitionOwners) {
    partitionOwnerList = Lists.newArrayList(partitionOwners);
  }

  @Override
  public Collection<PartitionOwner> generateChangedPartitionOwners(
      Collection<PartitionStats> allPartitionStatsList,
      Collection<WorkerInfo> availableWorkers,
      int maxWorkers,
      long superstep) {
    return PartitionBalancer.balancePartitionsAcrossWorkers(conf,
        partitionOwnerList, allPartitionStatsList, availableWorkers);
  }

  @Override
  public Collection<PartitionOwner> getCurrentPartitionOwners() {
    return partitionOwnerList;
  }

  @Override
  public PartitionStats createPartitionStats() {
    return new PartitionStats();
  }

  /**
   * Calculates worker that should be responsible for passed partition.
   *
   * @param partition Current partition
   * @param partitionCount Number of partitions
   * @param workerCount Number of workers
   * @return index of worker responsible for current partition
   */
  protected abstract int getWorkerIndex(
      int partition, int partitionCount, int workerCount);
}
