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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

/**
 * Aggregates the messages to be send to partitions so they can be sent
 * in bulk.
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
  private Map<Integer, Map<I, Collection<M>>> messageCache =
      new HashMap<Integer, Map<I, Collection<M>>>();
  /** Number of messages in each partition */
  private final Map<Integer, Integer> messageCountMap =
      new HashMap<Integer, Integer>();

  /**
   * Constructor
   *
   * @param conf Configuration used for instantiating the combiner.
   */
  public SendMessageCache(Configuration conf) {
    if (BspUtils.getVertexCombinerClass(conf) == null) {
      this.combiner = null;
    } else {
      this.combiner = BspUtils.createVertexCombiner(conf);
    }
  }

  /**
   * Add a message to the cache.
   *
   * @param partitionId Partition id
   * @param destVertexId Destination vertex id
   * @param message Message to be added
   * @return Number of messages in the partition.
   */
  public int addMessage(Integer partitionId, I destVertexId, M message) {
    // Get the message collection
    Map<I, Collection<M>> idMessagesMap = messageCache.get(partitionId);
    if (idMessagesMap == null) {
      idMessagesMap = new HashMap<I, Collection<M>>();
      messageCache.put(partitionId, idMessagesMap);
    }
    Collection<M> messages = idMessagesMap.get(destVertexId);
    if (messages == null) {
      messages = new ArrayList<M>(1);
      idMessagesMap.put(destVertexId, messages);
    }

    // Add the message
    int originalMessageCount = messages.size();
    messages.add(message);
    if (combiner != null) {
      try {
        messages = Lists.newArrayList(combiner.combine(destVertexId, messages));
      } catch (IOException e) {
        throw new IllegalStateException(
            "addMessage: Combiner failed to combine messages " + messages, e);
      }
      idMessagesMap.put(destVertexId, messages);
    }

    // Update the number of messages per partition
    Integer currentPartitionMessageCount = messageCountMap.get(partitionId);
    if (currentPartitionMessageCount == null) {
      currentPartitionMessageCount = 0;
    }
    Integer updatedPartitionMessageCount =
        currentPartitionMessageCount + messages.size() - originalMessageCount;
    messageCountMap.put(partitionId, updatedPartitionMessageCount);
    return updatedPartitionMessageCount;
  }

  /**
   * Gets the messages for a partition and removes it from the cache.
   *
   * @param partitionId Partition id
   * @return Removed partition messages
   */
  public Map<I, Collection<M>> removePartitionMessages(int partitionId) {
    Map<I, Collection<M>> idMessages = messageCache.remove(partitionId);
    messageCountMap.put(partitionId, 0);
    return idMessages;
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  public Map<Integer, Map<I, Collection<M>>> removeAllPartitionMessages() {
    Map<Integer, Map<I, Collection<M>>> allMessages = messageCache;
    messageCache = new HashMap<Integer, Map<I, Collection<M>>>();
    messageCountMap.clear();
    return allMessages;
  }
}
