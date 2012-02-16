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

package org.apache.giraph.graph.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Master will execute a hash based partitioning.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class HashMasterPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    MasterGraphPartitioner<I, V, E, M> {
  /** Multiplier for the current workers squared */
  public static final String PARTITION_COUNT_MULTIPLIER =
    "hash.masterPartitionCountMultipler";
  /** Default mulitplier for current workers squared */
  public static final float DEFAULT_PARTITION_COUNT_MULTIPLIER = 1.0f;
  /** Overrides default partition count calculation if not -1 */
  public static final String USER_PARTITION_COUNT =
    "hash.userPartitionCount";
  /** Default user partition count */
  public static final int DEFAULT_USER_PARTITION_COUNT = -1;
  /** Class logger */
  private static Logger LOG = Logger.getLogger(HashMasterPartitioner.class);
  /**
   * ZooKeeper has a limit of the data in a single znode of 1 MB and
   * each entry can go be on the average somewhat more than 300 bytes
   */
  private static final int MAX_PARTTIONS = 1024 * 1024 / 350;
  /** Provided configuration */
  private Configuration conf;
  /** Specified partition count (overrides calculation) */
  private final int userPartitionCount;
  /** Partition count (calculated in createInitialPartitionOwners) */
  private int partitionCount = -1;
  /** Save the last generated partition owner list */
  private List<PartitionOwner> partitionOwnerList;

  /**
   * Constructor.
   *
   *@param conf Configuration used.
   */
  public HashMasterPartitioner(Configuration conf) {
    this.conf = conf;
    userPartitionCount = conf.getInt(USER_PARTITION_COUNT,
        DEFAULT_USER_PARTITION_COUNT);
  }

  @Override
  public Collection<PartitionOwner> createInitialPartitionOwners(
      Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
    if (availableWorkerInfos.isEmpty()) {
      throw new IllegalArgumentException(
          "createInitialPartitionOwners: No available workers");
    }
    List<PartitionOwner> ownerList = new ArrayList<PartitionOwner>();
    Iterator<WorkerInfo> workerIt = availableWorkerInfos.iterator();
    if (userPartitionCount == DEFAULT_USER_PARTITION_COUNT) {
      float multiplier = conf.getFloat(
          PARTITION_COUNT_MULTIPLIER,
          DEFAULT_PARTITION_COUNT_MULTIPLIER);
      partitionCount =
          Math.max((int) (multiplier * availableWorkerInfos.size() *
              availableWorkerInfos.size()),
              1);
    } else {
      partitionCount = userPartitionCount;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("createInitialPartitionOwners: Creating " +
        partitionCount + ", default would have been " +
        (availableWorkerInfos.size() *
         availableWorkerInfos.size()) + " partitions.");
    }
    if (partitionCount > MAX_PARTTIONS) {
      LOG.warn("createInitialPartitionOwners: " +
          "Reducing the partitionCount to " + MAX_PARTTIONS +
          " from " + partitionCount);
      partitionCount = MAX_PARTTIONS;
    }

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
