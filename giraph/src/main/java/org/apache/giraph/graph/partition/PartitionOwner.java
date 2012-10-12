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

import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.io.Writable;

/**
 * Metadata about ownership of a partition.
 */
public interface PartitionOwner extends Writable {
  /**
   * Get the partition id that maps to the relevant {@link Partition} object
   *
   * @return Partition id
   */
  int getPartitionId();

  /**
   * Get the worker information that is currently responsible for
   * the partition id.
   *
   * @return Owning worker information.
   */
  WorkerInfo getWorkerInfo();

  /**
   * Set the current worker info.
   *
   * @param workerInfo Worker info responsible for partition
   */
  void setWorkerInfo(WorkerInfo workerInfo);

  /**
   * Get the worker information that was previously responsible for the
   * partition id.
   *
   * @return Owning worker information or null if no previous worker info.
   */
  WorkerInfo getPreviousWorkerInfo();

  /**
   * Set the previous worker info.
   *
   * @param workerInfo Worker info that was previously responsible for the
   *        partition.
   */
  void setPreviousWorkerInfo(WorkerInfo workerInfo);

  /**
   * If this is a restarted checkpoint, the worker will use this information
   * to determine where the checkpointed partition was stored on HDFS.
   *
   * @return Prefix of the checkpoint HDFS files for this partition, null if
   *         this is not a restarted superstep.
   */
  String getCheckpointFilesPrefix();

  /**
   * Set the checkpoint files prefix.  Master uses this.
   *
   * @param checkpointFilesPrefix HDFS checkpoint file prefix
   */
  void setCheckpointFilesPrefix(String checkpointFilesPrefix);
}
