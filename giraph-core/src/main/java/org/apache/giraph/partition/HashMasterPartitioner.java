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
 * Master will execute a hash based partitioning.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class HashMasterPartitioner<I extends WritableComparable,
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
   *@param conf Configuration used.
   */
  public HashMasterPartitioner(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public Collection<PartitionOwner> createInitialPartitionOwners(
      Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
    int partitionCount = PartitionUtils.computePartitionCount(
        availableWorkerInfos, maxWorkers, conf);
    List<PartitionOwner> ownerList = new ArrayList<PartitionOwner>();
    Iterator<WorkerInfo> workerIt = availableWorkerInfos.iterator();
    for (int i = 0; i < partitionCount; ++i) {
      PartitionOwner owner = new BasicPartitionOwner(i, workerIt.next());
      if (!workerIt.hasNext()) {
        workerIt = availableWorkerInfos.iterator();
      }
      ownerList.add(owner);
    }
    this.partitionOwnerList = ownerList;
    return ownerList;
  }

  @Override
  public void setPartitionOwners(Collection<PartitionOwner> partitionOwners) {
    this.partitionOwnerList = Lists.newArrayList(partitionOwners);
  }

  @Override
  public Collection<PartitionOwner> getCurrentPartitionOwners() {
    return partitionOwnerList;
  }

  /**
   * Subclasses can set the partition owner list.
   *
   * @param partitionOwnerList New partition owner list.
   */
  protected void setPartitionOwnerList(List<PartitionOwner>
  partitionOwnerList) {
    this.partitionOwnerList = partitionOwnerList;
  }

  @Override
  public Collection<PartitionOwner> generateChangedPartitionOwners(
      Collection<PartitionStats> allPartitionStatsList,
      Collection<WorkerInfo> availableWorkerInfos,
      int maxWorkers,
      long superstep) {
    return PartitionBalancer.balancePartitionsAcrossWorkers(
        conf,
        partitionOwnerList,
        allPartitionStatsList,
        availableWorkerInfos);
  }

  @Override
  public PartitionStats createPartitionStats() {
    return new PartitionStats();
  }


}
