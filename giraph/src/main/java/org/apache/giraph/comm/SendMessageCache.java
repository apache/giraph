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

package org.apache.giraph.comm;

import java.util.List;
import java.util.Map;

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Aggregates the messages to be send to workers so they can be sent
 * in bulk.  Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SendMessageCache<I extends WritableComparable,
    M extends Writable> {
  /** Internal cache */
  private final VertexIdMessageCollection<I, M>[] messageCache;
  /** Number of messages in each partition */
  private final int[] messageCounts;
  /** List of partition ids belonging to a worker */
  private final Map<WorkerInfo, List<Integer>> workerPartitions =
      Maps.newHashMap();
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   */
  public SendMessageCache(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?, ?> serviceWorker) {
    this.conf = conf;

    int maxPartition = 0;
    for (PartitionOwner partitionOwner : serviceWorker.getPartitionOwners()) {
      List<Integer> workerPartitionIds =
          workerPartitions.get(partitionOwner.getWorkerInfo());
      if (workerPartitionIds == null) {
        workerPartitionIds = Lists.newArrayList();
        workerPartitions.put(partitionOwner.getWorkerInfo(),
            workerPartitionIds);
      }
      workerPartitionIds.add(partitionOwner.getPartitionId());
      maxPartition = Math.max(partitionOwner.getPartitionId(), maxPartition);
    }
    messageCache = new VertexIdMessageCollection[maxPartition + 1];

    int maxWorker = 0;
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      maxWorker = Math.max(maxWorker, workerInfo.getTaskId());
    }
    messageCounts = new int[maxWorker + 1];
  }

  /**
   * Add a message to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param message Message to be send to remote
   *                <b>host => partition => vertex</b>
   * @return Number of messages in the partition.
   */
  public int addMessage(WorkerInfo workerInfo,
    final int partitionId, I destVertexId, M message) {
    // Get the message collection
    VertexIdMessageCollection<I, M> partitionMessages =
        messageCache[partitionId];
    if (partitionMessages == null) {
      partitionMessages = new VertexIdMessageCollection<I, M>(conf);
      partitionMessages.initialize();
      messageCache[partitionId] = partitionMessages;
    }
    partitionMessages.add(destVertexId, message);

    // Update the number of cached, outgoing messages per worker
    messageCounts[workerInfo.getTaskId()]++;
    return messageCounts[workerInfo.getTaskId()];
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return List of pairs (partitionId, VertexIdMessageCollection),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, VertexIdMessageCollection<I, M>>
  removeWorkerMessages(WorkerInfo workerInfo) {
    PairList<Integer, VertexIdMessageCollection<I, M>> workerMessages =
        new PairList<Integer, VertexIdMessageCollection<I, M>>();
    workerMessages.initialize();
    for (Integer partitionId : workerPartitions.get(workerInfo)) {
      if (messageCache[partitionId] != null) {
        workerMessages.add(partitionId, messageCache[partitionId]);
        messageCache[partitionId] = null;
      }
    }
    messageCounts[workerInfo.getTaskId()] = 0;
    return workerMessages;
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  public PairList<WorkerInfo, PairList<
      Integer, VertexIdMessageCollection<I, M>>> removeAllMessages() {
    PairList<WorkerInfo, PairList<Integer, VertexIdMessageCollection<I, M>>>
        allMessages = new PairList<WorkerInfo,
        PairList<Integer, VertexIdMessageCollection<I, M>>>();
    allMessages.initialize();
    for (WorkerInfo workerInfo : workerPartitions.keySet()) {
      PairList<Integer, VertexIdMessageCollection<I, M>> workerMessages =
          removeWorkerMessages(workerInfo);
      if (!workerMessages.isEmpty()) {
        allMessages.add(workerInfo, workerMessages);
      }
      messageCounts[workerInfo.getTaskId()] = 0;
    }
    return allMessages;
  }
}
