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

package org.apache.giraph.graph;

import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.master.MasterInfo;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Helper class to write descriptions of master, workers and partition owners
 */
public class AddressesAndPartitionsWritable implements Writable {
  /** Master information */
  private MasterInfo masterInfo;
  /** List of all workers */
  private List<WorkerInfo> workerInfos;
  /** Collection of partitions */
  private Collection<PartitionOwner> partitionOwners;

  /**
   * Constructor when we want to serialize object
   *
   * @param masterInfo Master information
   * @param workerInfos List of all workers
   * @param partitionOwners Collection of partitions
   */
  public AddressesAndPartitionsWritable(MasterInfo masterInfo,
      List<WorkerInfo> workerInfos,
      Collection<PartitionOwner> partitionOwners) {
    this.masterInfo = masterInfo;
    this.workerInfos = workerInfos;
    this.partitionOwners = partitionOwners;
  }

  /** Constructor for reflection */
  public AddressesAndPartitionsWritable() {
  }

  /**
   * Get master information
   *
   * @return Master information
   */
  public MasterInfo getMasterInfo() {
    return masterInfo;
  }

  /**
   * Get all workers
   *
   * @return List of all workers
   */
  public List<WorkerInfo> getWorkerInfos() {
    return workerInfos;
  }

  /**
   * Get partition owners
   *
   * @return Collection of partition owners
   */
  public Collection<PartitionOwner> getPartitionOwners() {
    return partitionOwners;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    masterInfo.write(output);

    output.writeInt(workerInfos.size());
    for (WorkerInfo workerInfo : workerInfos) {
      workerInfo.write(output);
    }

    Map<Integer, WorkerInfo> workerInfoMap = getAsWorkerInfoMap(workerInfos);
    // Also write out the previous worker information that are used
    // in the partition owners
    List<WorkerInfo> previousWorkerInfos = Lists.newArrayList();
    for (PartitionOwner partitionOwner : partitionOwners) {
      if (partitionOwner.getPreviousWorkerInfo() != null) {
        if (!workerInfoMap.containsKey(
            partitionOwner.getPreviousWorkerInfo().getTaskId())) {
          previousWorkerInfos.add(partitionOwner.getPreviousWorkerInfo());
        }
      }
    }
    output.writeInt(previousWorkerInfos.size());
    for (WorkerInfo workerInfo : previousWorkerInfos) {
      workerInfo.write(output);
    }

    output.writeInt(partitionOwners.size());
    if (partitionOwners.size() > 0) {
      WritableUtils.writeClass(
          partitionOwners.iterator().next().getClass(), output);
    }
    for (PartitionOwner partitionOwner : partitionOwners) {
      partitionOwner.writeWithWorkerIds(output);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    masterInfo = new MasterInfo();
    masterInfo.readFields(input);

    int workerInfosSize = input.readInt();
    workerInfos = Lists.newArrayListWithCapacity(workerInfosSize);
    for (int i = 0; i < workerInfosSize; i++) {
      WorkerInfo workerInfo = new WorkerInfo();
      workerInfo.readFields(input);
      workerInfos.add(workerInfo);
    }

    Map<Integer, WorkerInfo> workerInfoMap = getAsWorkerInfoMap(workerInfos);
    int additionalWorkerInfos = input.readInt();
    for (int i = 0; i < additionalWorkerInfos; i++) {
      WorkerInfo workerInfo = new WorkerInfo();
      workerInfo.readFields(input);
      workerInfoMap.put(workerInfo.getTaskId(), workerInfo);
    }

    int partitionOwnersSize = input.readInt();
    Class<PartitionOwner> partitionOwnerClass = null;
    if (partitionOwnersSize > 0) {
      partitionOwnerClass = WritableUtils.readClass(input);
    }
    partitionOwners = Lists.newArrayListWithCapacity(partitionOwnersSize);
    for (int i = 0; i < partitionOwnersSize; i++) {
      PartitionOwner partitionOwner =
          ReflectionUtils.newInstance(partitionOwnerClass);
      partitionOwner.readFieldsWithWorkerIds(input, workerInfoMap);
      partitionOwners.add(partitionOwner);
    }
  }

  /**
   * Convert Iterable of WorkerInfos to the map from task id to WorkerInfo.
   *
   * @param workerInfos Iterable of WorkerInfos
   * @return The map from task id to WorkerInfo
   */
  private static Map<Integer, WorkerInfo> getAsWorkerInfoMap(
      Iterable<WorkerInfo> workerInfos) {
    Map<Integer, WorkerInfo> workerInfoMap =
        Maps.newHashMapWithExpectedSize(Iterables.size(workerInfos));
    for (WorkerInfo workerInfo : workerInfos) {
      workerInfoMap.put(workerInfo.getTaskId(), workerInfo);
    }
    return workerInfoMap;
  }
}
