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
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.comm.aggregators.OwnerAggregatorServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.partition.DiskBackedPartitionStore;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.SimplePartitionStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Anything that the server stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class ServerData<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Partition store for this worker. */
  private volatile PartitionStore<I, V, E> partitionStore;
  /** Edge store for this worker. */
  private final EdgeStore<I, V, E> edgeStore;
  /** Message store factory */
  private final MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
  messageStoreFactory;
  /**
   * Message store for incoming messages (messages which will be consumed
   * in the next super step)
   */
  private volatile MessageStore<I, Writable> incomingMessageStore;
  /**
   * Message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   */
  private volatile MessageStore<I, Writable> currentMessageStore;
  /**
   * Map of partition ids to incoming vertex mutations from other workers.
   * (Synchronized access to values)
   */
  private final ConcurrentHashMap<I, VertexMutations<I, V, E>>
  vertexMutations = new ConcurrentHashMap<I, VertexMutations<I, V, E>>();
  /**
   * Holds aggregtors which current worker owns from current superstep
   */
  private final OwnerAggregatorServerData ownerAggregatorData;
  /**
   * Holds old aggregators from previous superstep
   */
  private final AllAggregatorServerData allAggregatorData;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param conf Configuration
   * @param messageStoreFactory Factory for message stores
   * @param context Mapper context
   */
  public ServerData(
      CentralizedServiceWorker<I, V, E> service,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
          messageStoreFactory,
      Mapper<?, ?, ?, ?>.Context context) {
    this.serviceWorker = service;
    this.conf = conf;
    this.messageStoreFactory = messageStoreFactory;
    if (GiraphConstants.USE_OUT_OF_CORE_GRAPH.get(conf)) {
      partitionStore =
          new DiskBackedPartitionStore<I, V, E>(conf, context);
    } else {
      partitionStore =
          new SimplePartitionStore<I, V, E>(conf, context);
    }
    edgeStore = new EdgeStore<I, V, E>(service, conf, context);
    ownerAggregatorData = new OwnerAggregatorServerData(context, conf);
    allAggregatorData = new AllAggregatorServerData(context, conf);
  }

  public EdgeStore<I, V, E> getEdgeStore() {
    return edgeStore;
  }

  /**
   * Return the partition store for this worker.
   *
   * @return The partition store
   */
  public PartitionStore<I, V, E> getPartitionStore() {
    return partitionStore;
  }

  /**
   * Get message store for incoming messages (messages which will be consumed
   * in the next super step)
   *
   * @param <M> Message data
   * @return Incoming message store
   */
  public <M extends Writable> MessageStore<I, M> getIncomingMessageStore() {
    return (MessageStore<I, M>) incomingMessageStore;
  }

  /**
   * Get message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   *
   * @param <M> Message data
   * @return Current message store
   */
  public <M extends Writable> MessageStore<I, M> getCurrentMessageStore() {
    return (MessageStore<I, M>) currentMessageStore;
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
    currentMessageStore =
        incomingMessageStore != null ? incomingMessageStore :
            messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
    incomingMessageStore =
        messageStoreFactory.newStore(conf.getOutgoingMessageValueFactory());
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentHashMap<I, VertexMutations<I, V, E>>
  getVertexMutations() {
    return vertexMutations;
  }

  /**
   * Get holder for aggregators which current worker owns
   *
   * @return Holder for aggregators which current worker owns
   */
  public OwnerAggregatorServerData getOwnerAggregatorData() {
    return ownerAggregatorData;
  }

  /**
   * Get holder for aggregators from previous superstep
   *
   * @return Holder for aggregators from previous superstep
   */
  public AllAggregatorServerData getAllAggregatorData() {
    return allAggregatorData;
  }

  /**
   * Get the reference of the service worker.
   *
   * @return CentralizedServiceWorker
   */
  public CentralizedServiceWorker<I, V, E> getServiceWorker() {
    return this.serviceWorker;
  }
}
