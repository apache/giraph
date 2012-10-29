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

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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
  /** Combiner instance, can be null */
  private final VertexCombiner<I, M> combiner;
  /** Internal cache */
  private Map<WorkerInfo, Map<Integer, VertexIdMessageCollection<I, M>>>
  messageCache =
      new HashMap<WorkerInfo, Map<Integer, VertexIdMessageCollection<I, M>>>();
  /** Number of messages in each partition */
  private final Map<WorkerInfo, Integer> messageCountMap =
      new HashMap<WorkerInfo, Integer>();
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param conf Configuration used for instantiating the combiner.
   */
  public SendMessageCache(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
    if (conf.getVertexCombinerClass() == null) {
      this.combiner = null;
    } else {
      this.combiner = conf.createVertexCombiner();
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
   * @return Number of messages in the partition.
   */
  public int addMessage(WorkerInfo workerInfo,
    final int partitionId, I destVertexId, M message) {
    // Get the message collection
    Map<Integer, VertexIdMessageCollection<I, M>> partitionMap =
      messageCache.get(workerInfo);
    if (partitionMap == null) {
      partitionMap = new HashMap<Integer, VertexIdMessageCollection<I, M>>();
      messageCache.put(workerInfo, partitionMap);
    }
    VertexIdMessageCollection<I, M> vertexMessages =
        partitionMap.get(partitionId);

    if (vertexMessages == null) {
      vertexMessages = new VertexIdMessageCollection<I, M>(conf);
      vertexMessages.initialize();
      partitionMap.put(partitionId, vertexMessages);
    }
    vertexMessages.add(destVertexId, message);

    // Update the number of cached, outgoing messages per worker
    Integer currentWorkerMessageCount = messageCountMap.get(workerInfo);
    if (currentWorkerMessageCount == null) {
      currentWorkerMessageCount = 0;
    }
    final int updatedWorkerMessageCount =
        currentWorkerMessageCount + 1;
    messageCountMap.put(workerInfo, updatedWorkerMessageCount);
    return updatedWorkerMessageCount;
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return Map of all messages (keyed by partition ID's) destined
   *         for vertices hosted by <code>workerInfo</code>
   */
  public Map<Integer, VertexIdMessageCollection<I, M>> removeWorkerMessages(
      WorkerInfo workerInfo) {
    Map<Integer, VertexIdMessageCollection<I, M>> workerMessages =
        messageCache.remove(workerInfo);
    messageCountMap.put(workerInfo, 0);
    return workerMessages;
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  public Map<WorkerInfo, Map<
      Integer, VertexIdMessageCollection<I, M>>> removeAllMessages() {
    Map<WorkerInfo, Map<Integer, VertexIdMessageCollection<I, M>>>
        allMessages = messageCache;
    messageCache =
        new HashMap<WorkerInfo,
            Map<Integer, VertexIdMessageCollection<I, M>>>();
    messageCountMap.clear();
    return allMessages;
  }
}
