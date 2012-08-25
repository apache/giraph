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

import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.partition.DiskBackedPartitionStore;
import org.apache.giraph.graph.partition.PartitionStore;
import org.apache.giraph.graph.partition.SimplePartitionStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Anything that the server stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class ServerData<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /** Partition store for this worker. */
  private volatile PartitionStore<I, V, E, M> partitionStore;
  /** Message store factory */
  private final
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> messageStoreFactory;
  /**
   * Message store for incoming messages (messages which will be consumed
   * in the next super step)
   */
  private volatile MessageStoreByPartition<I, M> incomingMessageStore;
  /**
   * Message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   */
  private volatile MessageStoreByPartition<I, M> currentMessageStore;
  /**
   * Map of partition ids to incoming vertex mutations from other workers.
   * (Synchronized access to values)
   */
  private final ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  vertexMutations = new ConcurrentHashMap<I, VertexMutations<I, V, E, M>>();

  /**
   * Constructor.
   *
   * @param configuration Configuration
   * @param messageStoreFactory Factory for message stores
   */
  public ServerData(Configuration configuration,
                    MessageStoreFactory<I, M, MessageStoreByPartition<I, M>>
                        messageStoreFactory) {

    this.messageStoreFactory = messageStoreFactory;
    currentMessageStore = messageStoreFactory.newStore();
    incomingMessageStore = messageStoreFactory.newStore();
    if (configuration.getBoolean(GiraphJob.USE_OUT_OF_CORE_GRAPH,
        GiraphJob.USE_OUT_OF_CORE_GRAPH_DEFAULT)) {
      partitionStore = new DiskBackedPartitionStore<I, V, E, M>(configuration);
    } else {
      partitionStore = new SimplePartitionStore<I, V, E, M>(configuration);
    }
  }

  /**
   * Return the partition store for this worker.
   *
   * @return The partition store
   */
  public PartitionStore<I, V, E, M> getPartitionStore() {
    return partitionStore;
  }

  /**
   * Get message store for incoming messages (messages which will be consumed
   * in the next super step)
   *
   * @return Incoming message store
   */
  public MessageStoreByPartition<I, M> getIncomingMessageStore() {
    return incomingMessageStore;
  }

  /**
   * Get message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   *
   * @return Current message store
   */
  public MessageStoreByPartition<I, M> getCurrentMessageStore() {
    return currentMessageStore;
  }

  /** Prepare for next super step */
  public void prepareSuperstep() {
    if (currentMessageStore != null) {
      try {
        currentMessageStore.clearAll();
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to clear previous message store");
      }
    }
    currentMessageStore = incomingMessageStore;
    incomingMessageStore = messageStoreFactory.newStore();
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  getVertexMutations() {
    return vertexMutations;
  }
}
