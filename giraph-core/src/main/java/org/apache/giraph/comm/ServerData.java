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

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.data.DiskBackedEdgeStore;
import org.apache.giraph.ooc.data.DiskBackedMessageStore;
import org.apache.giraph.ooc.data.DiskBackedPartitionStore;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.SimplePartitionStore;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

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
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ServerData.class);
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
   * Map of partition ids to vertex mutations from other workers. These are
   * mutations that should be applied before execution of *current* super step.
   * (accesses to keys should be thread-safe as multiple threads may resolve
   * mutations of different partitions at the same time)
   */
  private ConcurrentMap<Integer,
      ConcurrentMap<I, VertexMutations<I, V, E>>>
      oldPartitionMutations = Maps.newConcurrentMap();
  /**
   * Map of partition ids to vertex mutations from other workers. These are
   * mutations that are coming from other workers as the execution goes one in a
   * super step. These mutations should be applied in the *next* super step.
   * (this should be thread-safe)
   */
  private ConcurrentMap<Integer,
      ConcurrentMap<I, VertexMutations<I, V, E>>>
      partitionMutations = Maps.newConcurrentMap();
  /**
   * Holds aggregators which current worker owns from current superstep
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

  /** Job context (for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param workerServer Worker server
   * @param conf Configuration
   * @param context Mapper context
   */
  public ServerData(
      CentralizedServiceWorker<I, V, E> service,
      WorkerServer workerServer,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.serviceWorker = service;
    this.conf = conf;
    this.messageStoreFactory = createMessageStoreFactory();
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(service, conf, context);
    EdgeStore<I, V, E> inMemoryEdgeStore = edgeStoreFactory.newStore();
    PartitionStore<I, V, E> inMemoryPartitionStore =
        new SimplePartitionStore<I, V, E>(conf, context);
    if (GiraphConstants.USE_OUT_OF_CORE_GRAPH.get(conf)) {
      oocEngine = new OutOfCoreEngine(conf, service, workerServer);
      partitionStore =
          new DiskBackedPartitionStore<I, V, E>(inMemoryPartitionStore,
              conf, context, oocEngine);
      edgeStore =
          new DiskBackedEdgeStore<I, V, E>(inMemoryEdgeStore, conf, oocEngine);
    } else {
      partitionStore = inMemoryPartitionStore;
      edgeStore = inMemoryEdgeStore;
      oocEngine = null;
    }
    ownerAggregatorData = new OwnerAggregatorServerData(context);
    allAggregatorData = new AllAggregatorServerData(context, conf);
    this.context = context;
  }

  /**
   * Decide which message store should be used for current application,
   * and create the factory for that store
   *
   * @return Message store factory
   */
  private MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
  createMessageStoreFactory() {
    Class<? extends MessageStoreFactory> messageStoreFactoryClass =
        MESSAGE_STORE_FACTORY_CLASS.get(conf);

    MessageStoreFactory messageStoreFactoryInstance =
        ReflectionUtils.newInstance(messageStoreFactoryClass);
    messageStoreFactoryInstance.initialize(serviceWorker, conf);

    return messageStoreFactoryInstance;
  }

  /**
   * Return the out-of-core engine for this worker.
   *
   * @return The out-of-core engine
   */
  public OutOfCoreEngine getOocEngine() {
    return oocEngine;
  }

  /**
   * Return the edge store for this worker.
   *
   * @return The edge store
   */
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
   */
  public void resetMessageStores() {
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

  /** Prepare for next superstep */
  public void prepareSuperstep() {
    if (currentMessageStore != null) {
      currentMessageStore.clearAll();
    }

    MessageStore<I, Writable> nextCurrentMessageStore;
    MessageStore<I, Writable> nextIncomingMessageStore;
    MessageStore<I, Writable> messageStore;

    // First create the necessary in-memory message stores. If out-of-core
    // mechanism is enabled, we wrap the in-memory message stores within
    // disk-backed messages stores.
    if (incomingMessageStore != null) {
      nextCurrentMessageStore = incomingMessageStore;
    } else {
      messageStore = messageStoreFactory.newStore(
          conf.getIncomingMessageClasses());
      if (oocEngine == null) {
        nextCurrentMessageStore = messageStore;
      } else {
        nextCurrentMessageStore = new DiskBackedMessageStore<>(
            conf, oocEngine, messageStore,
            conf.getIncomingMessageClasses().useMessageCombiner(),
            serviceWorker.getSuperstep());
      }
    }

    messageStore = messageStoreFactory.newStore(
        conf.getOutgoingMessageClasses());
    if (oocEngine == null) {
      nextIncomingMessageStore = messageStore;
    } else {
      nextIncomingMessageStore = new DiskBackedMessageStore<>(
          conf, oocEngine, messageStore,
          conf.getOutgoingMessageClasses().useMessageCombiner(),
          serviceWorker.getSuperstep() + 1);
    }

    // If out-of-core engine is enabled, we avoid overlapping of out-of-core
    // decisions with change of superstep. This avoidance is done to simplify
    // the design and reduce excessive use of synchronization primitives.
    if (oocEngine != null) {
      oocEngine.getSuperstepLock().writeLock().lock();
    }
    currentMessageStore = nextCurrentMessageStore;
    incomingMessageStore = nextIncomingMessageStore;
    if (oocEngine != null) {
      oocEngine.reset();
      oocEngine.getSuperstepLock().writeLock().unlock();
    }
    currentMessageStore.finalizeStore();

    currentWorkerToWorkerMessages = incomingWorkerToWorkerMessages;
    incomingWorkerToWorkerMessages =
        Collections.synchronizedList(new ArrayList<Writable>());
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentMap<Integer, ConcurrentMap<I, VertexMutations<I, V, E>>>
  getPartitionMutations() {
    return partitionMutations;
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

  /**
   * Prepare resolving mutation.
   */
  public void prepareResolveMutations() {
    oldPartitionMutations = partitionMutations;
    partitionMutations = Maps.newConcurrentMap();
  }

  /**
   * Resolve mutations specific for a partition. This method is called once
   * per partition, before the computation for that partition starts.
   * @param partition The partition to resolve mutations for
   */
  public void resolvePartitionMutation(Partition<I, V, E> partition) {
    Integer partitionId = partition.getId();
    VertexResolver<I, V, E> vertexResolver = conf.createVertexResolver();
    ConcurrentMap<I, VertexMutations<I, V, E>> prevPartitionMutations =
        oldPartitionMutations.get(partitionId);

    boolean ignoreExistingVertices =
        conf.getIncomingMessageClasses().ignoreExistingVertices();

    // Resolve mutations that are explicitly sent for this partition
    if (prevPartitionMutations != null) {
      for (Map.Entry<I, VertexMutations<I, V, E>> entry : prevPartitionMutations
          .entrySet()) {
        I vertexId = entry.getKey();
        Vertex<I, V, E> originalVertex = partition.getVertex(vertexId);
        VertexMutations<I, V, E> vertexMutations = entry.getValue();
        Vertex<I, V, E> vertex = vertexResolver.resolve(vertexId,
            originalVertex, vertexMutations,
            !ignoreExistingVertices &&
            getCurrentMessageStore().hasMessagesForVertex(entry.getKey()));

        if (LOG.isDebugEnabled()) {
          LOG.debug("resolvePartitionMutations: Resolved vertex index " +
              vertexId + " in partition index " + partitionId +
              " with original vertex " + originalVertex +
              ", returned vertex " + vertex + " on superstep " +
              serviceWorker.getSuperstep() + " with mutations " +
              vertexMutations);
        }

        if (vertex != null) {
          partition.putVertex(vertex);
        } else if (originalVertex != null) {
          partition.removeVertex(vertexId);
          if (!ignoreExistingVertices) {
            getCurrentMessageStore().clearVertexMessages(vertexId);
          }
        }
        context.progress();
      }
    }

    if (!ignoreExistingVertices) {
      // Keep track of vertices which are not here in the partition, but have
      // received messages
      Iterable<I> destinations = getCurrentMessageStore().
          getPartitionDestinationVertices(partitionId);
      if (!Iterables.isEmpty(destinations)) {
        for (I vertexId : destinations) {
          if (partition.getVertex(vertexId) == null) {
            Vertex<I, V, E> vertex =
                vertexResolver.resolve(vertexId, null, null, true);

            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "resolvePartitionMutations: A non-existing vertex has " +
                  "message(s). Added vertex index " + vertexId +
                  " in partition index " + partitionId +
                  ", vertex = " + vertex + ", on superstep " +
                  serviceWorker.getSuperstep());
            }

            if (vertex != null) {
              partition.putVertex(vertex);
            }
            context.progress();
          }
        }
      }
    }
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
}
