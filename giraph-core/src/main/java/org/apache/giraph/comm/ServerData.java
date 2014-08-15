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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.comm.aggregators.OwnerAggregatorServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.queue.AsyncMessageStoreWrapper;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.partition.DiskBackedPartitionStore;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.SimplePartitionStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

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

  /** Store for current messages from other workers to this worker */
  private volatile List<Writable> currentWorkerToWorkerMessages =
      Collections.synchronizedList(new ArrayList<Writable>());
  /** Store for message from other workers to this worker for next superstep */
  private volatile List<Writable> incomingWorkerToWorkerMessages =
      Collections.synchronizedList(new ArrayList<Writable>());

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
          new DiskBackedPartitionStore<I, V, E>(conf, context,
              getServiceWorker());
    } else {
      partitionStore =
          new SimplePartitionStore<I, V, E>(conf, context);
    }
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(service, conf, context);
    edgeStore = edgeStoreFactory.newStore();
    ownerAggregatorData = new OwnerAggregatorServerData(context);
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

  /**
   * Re-initialize message stores.
   * Discards old values if any.
   * @throws IOException
   */
  public void resetMessageStores() throws IOException {
    if (currentMessageStore != null) {
      currentMessageStore.clearAll();
      currentMessageStore = null;
    }
    if (incomingMessageStore != null) {
      incomingMessageStore.clearAll();
      incomingMessageStore = null;
    }
    prepareSuperstep();
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
    // finalize current message-store before resolving mutations
    currentMessageStore.finalizeStore();

    currentWorkerToWorkerMessages = incomingWorkerToWorkerMessages;
    incomingWorkerToWorkerMessages =
        Collections.synchronizedList(new ArrayList<Writable>());
  }

  /**
   * In case of async message store we have to wait for all messages
   * to be processed before going into next superstep.
   */
  public void waitForComplete() {
    if (incomingMessageStore instanceof AsyncMessageStoreWrapper) {
      ((AsyncMessageStoreWrapper) incomingMessageStore).waitToComplete();
    }
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

  /**
   * Get and clear worker to worker messages for this superstep. Can be
   * called only once per superstep.
   *
   * @return List of messages for this worker
   */
  public List<Writable> getAndClearCurrentWorkerToWorkerMessages() {
    List<Writable> ret = currentWorkerToWorkerMessages;
    currentWorkerToWorkerMessages = null;
    return ret;
  }

  /**
   * Add incoming message to this worker for next superstep. Thread-safe.
   *
   * @param message Message received
   */
  public void addIncomingWorkerToWorkerMessage(Writable message) {
    incomingWorkerToWorkerMessages.add(message);
  }


  /**
   * Get worker to worker messages received in previous superstep.
   * @return list of current worker to worker messages.
   */
  public List<Writable> getCurrentWorkerToWorkerMessages() {
    return currentWorkerToWorkerMessages;
  }

}
