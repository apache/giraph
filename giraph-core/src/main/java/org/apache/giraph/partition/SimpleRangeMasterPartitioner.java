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


import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A range-based master partitioner where equal-sized ranges of partitions
 * are deterministically assigned to workers.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class SimpleRangeMasterPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    MasterGraphPartitioner<I, V, E> {
  /** Class logger */
  private static Logger LOG = Logger.getLogger(HashMasterPartitioner.class);
  /** Provided configuration */
  private ImmutableClassesGiraphConfiguration conf;
  /** Save the last generated partition owner list */
  private List<PartitionOwner> partitionOwnerList;

  /**
   * Constructor.
   *
   * @param conf Configuration used.
   */
  public SimpleRangeMasterPartitioner(
      ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public Collection<PartitionOwner> createInitialPartitionOwners(
      Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
    int partitionCount = PartitionUtils.computePartitionCount(
        availableWorkerInfos, maxWorkers, conf);
    int rangeSize = partitionCount / availableWorkerInfos.size();

    partitionOwnerList = new ArrayList<PartitionOwner>();
    Iterator<WorkerInfo> workerIt = availableWorkerInfos.iterator();
    WorkerInfo currentWorker = null;

    int i = 0;
    for (; i < partitionCount; ++i) {
      if (i % rangeSize == 0) {
        if (!workerIt.hasNext()) {
          break;
        }
        currentWorker = workerIt.next();
      }
      partitionOwnerList.add(new BasicPartitionOwner(i, currentWorker));
    }

    // Distribute the remainder among all workers.
    if (i < partitionCount) {
      workerIt = availableWorkerInfos.iterator();
      for (; i < partitionCount; ++i) {
        partitionOwnerList.add(new BasicPartitionOwner(i, workerIt.next()));
      }
    }

    return partitionOwnerList;
  }

  @Override
  public Collection<PartitionOwner> generateChangedPartitionOwners(
      Collection<PartitionStats> allPartitionStatsList,
      Collection<WorkerInfo> availableWorkers,
      int maxWorkers,
      long superstep) {
    return PartitionBalancer.balancePartitionsAcrossWorkers(
        conf,
        partitionOwnerList,
        allPartitionStatsList,
        availableWorkers);
  }

  @Override
  public Collection<PartitionOwner> getCurrentPartitionOwners() {
    return partitionOwnerList;
  }

  @Override
  public PartitionStats createPartitionStats() {
    return new PartitionStats();
  }
}
