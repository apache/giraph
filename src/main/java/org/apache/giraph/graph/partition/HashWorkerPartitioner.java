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
import org.apache.hadoop.io.WritableComparable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements hash-based partitioning from the id hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class HashWorkerPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerGraphPartitioner<I, V, E, M> {
  /** Mapping of the vertex ids to {@link PartitionOwner} */
  protected List<PartitionOwner> partitionOwnerList =
      new ArrayList<PartitionOwner>();

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
    synchronized (partitionOwnerList) {
      return partitionOwnerList.get(Math.abs(vertexId.hashCode()) %
          partitionOwnerList.size());
    }
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<I, V, E, M> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(
      WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners,
      PartitionStore<I, V, E, M> partitionStore) {
    synchronized (partitionOwnerList) {
      partitionOwnerList.clear();
      partitionOwnerList.addAll(masterSetPartitionOwners);
    }

    Set<WorkerInfo> dependentWorkerSet = new HashSet<WorkerInfo>();
    Map<WorkerInfo, List<Integer>> workerPartitionOwnerMap =
        new HashMap<WorkerInfo, List<Integer>>();
    for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
      if (partitionOwner.getPreviousWorkerInfo() == null) {
        continue;
      } else if (partitionOwner.getWorkerInfo().equals(
          myWorkerInfo) &&
          partitionOwner.getPreviousWorkerInfo().equals(
              myWorkerInfo)) {
        throw new IllegalStateException(
            "updatePartitionOwners: Impossible to have the same " +
                "previous and current worker info " + partitionOwner +
                " as me " + myWorkerInfo);
      } else if (partitionOwner.getWorkerInfo().equals(myWorkerInfo)) {
        dependentWorkerSet.add(partitionOwner.getPreviousWorkerInfo());
      } else if (partitionOwner.getPreviousWorkerInfo().equals(
          myWorkerInfo)) {
        if (workerPartitionOwnerMap.containsKey(
            partitionOwner.getWorkerInfo())) {
          workerPartitionOwnerMap.get(
              partitionOwner.getWorkerInfo()).add(
                  partitionOwner.getPartitionId());
        } else {
          List<Integer> tmpPartitionOwnerList = new ArrayList<Integer>();
          tmpPartitionOwnerList.add(partitionOwner.getPartitionId());
          workerPartitionOwnerMap.put(partitionOwner.getWorkerInfo(),
                                      tmpPartitionOwnerList);
        }
      }
    }

    return new PartitionExchange(dependentWorkerSet,
        workerPartitionOwnerMap);
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    synchronized (partitionOwnerList) {
      return partitionOwnerList;
    }
  }
}
