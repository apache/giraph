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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;

/**
 * Basic partition owner, can be subclassed for more complicated partition
 * owner implementations.
 */
public class BasicPartitionOwner implements PartitionOwner,
    ImmutableClassesGiraphConfigurable {
  /** Configuration */
  private ImmutableClassesGiraphConfiguration conf;
  /** Partition id */
  private int partitionId = -1;
  /** Owning worker information */
  private WorkerInfo workerInfo;
  /** Previous (if any) worker info */
  private WorkerInfo previousWorkerInfo;
  /** Checkpoint files prefix for this partition */
  private String checkpointFilesPrefix;

  /**
   * Default constructor.
   */
  public BasicPartitionOwner() { }

  /**
   * Constructor with partition id and worker info.
   *
   * @param partitionId Partition id of this partition.
   * @param workerInfo Owner of the partition.
   */
  public BasicPartitionOwner(int partitionId, WorkerInfo workerInfo) {
    this(partitionId, workerInfo, null, null);
  }

  /**
   * Constructor with partition id and worker info.
   *
   * @param partitionId Partition id of this partition.
   * @param workerInfo Owner of the partition.
   * @param previousWorkerInfo Previous owner of this partition.
   * @param checkpointFilesPrefix Prefix of the checkpoint files.
   */
  public BasicPartitionOwner(int partitionId,
                             WorkerInfo workerInfo,
                             WorkerInfo previousWorkerInfo,
                             String checkpointFilesPrefix) {
    this.partitionId = partitionId;
    this.workerInfo = workerInfo;
    this.previousWorkerInfo = previousWorkerInfo;
    this.checkpointFilesPrefix = checkpointFilesPrefix;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public void setWorkerInfo(WorkerInfo workerInfo) {
    this.workerInfo = workerInfo;
  }

  @Override
  public WorkerInfo getPreviousWorkerInfo() {
    return previousWorkerInfo;
  }

  @Override
  public void setPreviousWorkerInfo(WorkerInfo workerInfo) {
    this.previousWorkerInfo = workerInfo;
  }

  @Override
  public void writeWithWorkerIds(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    output.writeInt(workerInfo.getTaskId());
    if (previousWorkerInfo != null) {
      output.writeInt(previousWorkerInfo.getTaskId());
    } else {
      output.writeInt(-1);
    }
    if (checkpointFilesPrefix != null) {
      output.writeBoolean(true);
      output.writeUTF(checkpointFilesPrefix);
    } else {
      output.writeBoolean(false);
    }
  }

  @Override
  public void readFieldsWithWorkerIds(DataInput input,
      Map<Integer, WorkerInfo> workerInfoMap) throws IOException {
    partitionId = input.readInt();
    int workerId = input.readInt();
    workerInfo = workerInfoMap.get(workerId);
    int previousWorkerId = input.readInt();
    if (previousWorkerId != -1) {
      previousWorkerInfo = workerInfoMap.get(previousWorkerId);
    }
    boolean hasCheckpointFilePrefix = input.readBoolean();
    if (hasCheckpointFilePrefix) {
      checkpointFilesPrefix = input.readUTF();
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    partitionId = input.readInt();
    workerInfo = new WorkerInfo();
    workerInfo.readFields(input);
    boolean hasPreviousWorkerInfo = input.readBoolean();
    if (hasPreviousWorkerInfo) {
      previousWorkerInfo = new WorkerInfo();
      previousWorkerInfo.readFields(input);
    }
    boolean hasCheckpointFilePrefix = input.readBoolean();
    if (hasCheckpointFilePrefix) {
      checkpointFilesPrefix = input.readUTF();
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    workerInfo.write(output);
    if (previousWorkerInfo != null) {
      output.writeBoolean(true);
      previousWorkerInfo.write(output);
    } else {
      output.writeBoolean(false);
    }
    if (checkpointFilesPrefix != null) {
      output.writeBoolean(true);
      output.writeUTF(checkpointFilesPrefix);
    } else {
      output.writeBoolean(false);
    }
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return "(id=" + partitionId + ",cur=" + workerInfo + ",prev=" +
        previousWorkerInfo + ",ckpt_file=" + checkpointFilesPrefix + ")";
  }
}
