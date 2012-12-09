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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

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
  /**
   * How much bigger than the average per partition size to make initial per
   * partition buffers.
   * If this value is A, message request size is M,
   * and a worker has P partitions, than its initial partition buffer size
   * will be (M / P) * (1 + A).
   */
  public static final String ADDITIONAL_MSG_REQUEST_SIZE =
      "giraph.additionalMsgRequestSize";
  /**
   * Default factor for how bigger should initial per partition buffers be
   * of 20%.
   */
  public static final float ADDITIONAL_MSG_REQUEST_SIZE_DEFAULT = 0.2f;

  /** Internal cache */
  private final ByteArrayVertexIdMessages<I, M>[] messageCache;
  /** Size of messages (in bytes) for each worker */
  private final int[] messageSizes;
  /** How big to initially make output streams for each worker's partitions */
  private final int[] initialBufferSizes;
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
    messageCache = new ByteArrayVertexIdMessages[maxPartition + 1];

    int maxWorker = 0;
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      maxWorker = Math.max(maxWorker, workerInfo.getTaskId());
    }
    messageSizes = new int[maxWorker + 1];

    float additionalRequestSize =
        conf.getFloat(ADDITIONAL_MSG_REQUEST_SIZE,
            ADDITIONAL_MSG_REQUEST_SIZE_DEFAULT);
    int requestSize = conf.getInt(GiraphConstants.MAX_MSG_REQUEST_SIZE,
        GiraphConstants.MAX_MSG_REQUEST_SIZE_DEFAULT);
    int initialRequestSize = (int) (requestSize * (1 + additionalRequestSize));
    initialBufferSizes = new int[maxWorker + 1];
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      initialBufferSizes[workerInfo.getTaskId()] =
          initialRequestSize / workerPartitions.get(workerInfo).size();
    }
  }

  /**
   * Add a message to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param message Message to be send to remote
   *                <b>host => partition => vertex</b>
   * @return Size of messages for the worker.
   */
  public int addMessage(WorkerInfo workerInfo,
    final int partitionId, I destVertexId, M message) {
    // Get the message collection
    ByteArrayVertexIdMessages<I, M> partitionMessages =
        messageCache[partitionId];
    int originalSize = 0;
    if (partitionMessages == null) {
      partitionMessages = new ByteArrayVertexIdMessages<I, M>();
      partitionMessages.setConf(conf);
      partitionMessages.initialize(initialBufferSizes[workerInfo.getTaskId()]);
      messageCache[partitionId] = partitionMessages;
    } else {
      originalSize = partitionMessages.getSize();
    }
    partitionMessages.add(destVertexId, message);

    // Update the size of cached, outgoing messages per worker
    messageSizes[workerInfo.getTaskId()] +=
        partitionMessages.getSize() - originalSize;
    return messageSizes[workerInfo.getTaskId()];
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return List of pairs (partitionId, ByteArrayVertexIdMessages),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, ByteArrayVertexIdMessages<I, M>>
  removeWorkerMessages(WorkerInfo workerInfo) {
    PairList<Integer, ByteArrayVertexIdMessages<I, M>> workerMessages =
        new PairList<Integer, ByteArrayVertexIdMessages<I, M>>();
    List<Integer> partitions = workerPartitions.get(workerInfo);
    workerMessages.initialize(partitions.size());
    for (Integer partitionId : partitions) {
      if (messageCache[partitionId] != null) {
        workerMessages.add(partitionId, messageCache[partitionId]);
        messageCache[partitionId] = null;
      }
    }
    messageSizes[workerInfo.getTaskId()] = 0;
    return workerMessages;
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  public PairList<WorkerInfo, PairList<
      Integer, ByteArrayVertexIdMessages<I, M>>> removeAllMessages() {
    PairList<WorkerInfo, PairList<Integer,
        ByteArrayVertexIdMessages<I, M>>>
        allMessages = new PairList<WorkerInfo,
        PairList<Integer, ByteArrayVertexIdMessages<I, M>>>();
    allMessages.initialize(messageSizes.length);
    for (WorkerInfo workerInfo : workerPartitions.keySet()) {
      PairList<Integer, ByteArrayVertexIdMessages<I,
                M>> workerMessages =
          removeWorkerMessages(workerInfo);
      if (!workerMessages.isEmpty()) {
        allMessages.add(workerInfo, workerMessages);
      }
      messageSizes[workerInfo.getTaskId()] = 0;
    }
    return allMessages;
  }
}
