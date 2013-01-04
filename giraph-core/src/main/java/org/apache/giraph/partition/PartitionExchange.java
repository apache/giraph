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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.worker.WorkerInfo;

/**
 * Describes what is required to send and wait for in a potential partition
 * exchange between workers.
 */
public class PartitionExchange {
  /** Workers that I am dependent on before I can continue */
  private final Set<WorkerInfo> myDependencyWorkerSet;
  /** Workers that I need to sent partitions to */
  private final Map<WorkerInfo, List<Integer>> sendWorkerPartitionMap;

  /**
   * Only constructor.
   *
   * @param myDependencyWorkerSet All the workers I must wait for
   * @param sendWorkerPartitionMap Partitions I need to send to other workers
   */
  public PartitionExchange(
      Set<WorkerInfo> myDependencyWorkerSet,
      Map<WorkerInfo, List<Integer>> sendWorkerPartitionMap) {
    this.myDependencyWorkerSet = myDependencyWorkerSet;
    this.sendWorkerPartitionMap = sendWorkerPartitionMap;
  }

  /**
   * Get the workers that I must wait for
   *
   * @return Set of workers I must wait for
   */
  public Set<WorkerInfo> getMyDependencyWorkerSet() {
    return myDependencyWorkerSet;
  }

  /**
   * Get a mapping of worker to list of partition ids I need to send to.
   *
   * @return Mapping of worker to partition id list I will send to.
   */
  public Map<WorkerInfo, List<Integer>> getSendWorkerPartitionMap() {
    return sendWorkerPartitionMap;
  }

  /**
   * Is this worker involved in a partition exchange?  Receiving or sending?
   *
   * @return True if needs to be involved in the exchange, false otherwise.
   */
  public boolean doExchange() {
    return !myDependencyWorkerSet.isEmpty() ||
        !sendWorkerPartitionMap.isEmpty();
  }
}
