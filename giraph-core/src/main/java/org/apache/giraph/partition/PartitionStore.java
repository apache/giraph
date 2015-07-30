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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.queue.AsyncMessageStoreWrapper;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

/**
 * Structure that stores partitions for a worker. PartitionStore does not allow
 * random accesses to partitions except upon removal.
 * This structure is thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public abstract class PartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements PartitionData<I, V, E>, MessageData<I> {
  /** Configuration. */
  protected final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Context used to report progress */
  protected final Mapper<?, ?, ?, ?>.Context context;
  /** service worker reference */
  protected final CentralizedServiceWorker<I, V, E> serviceWorker;

  /** Edge store for this worker */
  protected final EdgeStore<I, V, E> edgeStore;

  /** Message store factory */
  protected MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
      messageStoreFactory;
  /**
   * Message store for incoming messages (messages which will be consumed
   * in the next super step)
   */
  protected volatile MessageStore<I, Writable> incomingMessageStore;
  /**
   * Message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   */
  protected volatile MessageStore<I, Writable> currentMessageStore;

  /**
   * Constructor for abstract partition store
   *
   * @param conf Job configuration
   * @param context Mapper context
   * @param serviceWorker Worker service
   */
  public PartitionStore(ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.context = context;
    this.serviceWorker = serviceWorker;
    this.messageStoreFactory = createMessageStoreFactory();
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(serviceWorker, conf, context);
    this.edgeStore = edgeStoreFactory.newStore();
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

  @Override
  public boolean isEmpty() {
    return getNumPartitions() == 0;
  }

  @Override
  public <M extends Writable> MessageStore<I, M> getIncomingMessageStore() {
    return (MessageStore<I, M>) incomingMessageStore;
  }

  @Override
  public <M extends Writable> MessageStore<I, M> getCurrentMessageStore() {
    return (MessageStore<I, M>) currentMessageStore;
  }

  @Override
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
    currentMessageStore = incomingMessageStore != null ?
        incomingMessageStore :
        messageStoreFactory.newStore(conf.getIncomingMessageClasses());
    incomingMessageStore =
        messageStoreFactory.newStore(conf.getOutgoingMessageClasses());
    // finalize current message-store before resolving mutations
    currentMessageStore.finalizeStore();
  }

  /**
   * Called at the end of the computation. Called from a single thread.
   */
  public void shutdown() { }

  /**
   * Called at the beginning of the computation. Called from a single thread.
   */
  public void initialize() { }

  /**
   * Start the iteration cycle to iterate over partitions. Note that each
   * iteration cycle *must* iterate over *all* partitions. Usually an iteration
   * cycle is necessary for
   *   1) moving edges (from edge store) to vertices after edge input splits are
   *      loaded in INPUT_SUPERSTEP,
   *   2) computing partitions in each superstep (called once per superstep),
   *   3) saving vertices/edges in the output superstep.
   *   4) any sort of populating a data-structure based on the partitions in
   *      this store.
   *
   * After an iteration is started, multiple threads can access the partition
   * store using {@link #getNextPartition()} to iterate over the partitions.
   * Each time {@link #getNextPartition()} is called an unprocessed partition in
   * the current iteration is returned. After processing of the partition is
   * done, partition should be put back in the store using
   * {@link #putPartition(Partition)}. Here is an example of the entire
   * workflow:
   *
   * In the main thread:
   *   partitionStore.startIteration();
   *
   * In multiple threads iterating over the partitions:
   *   Partition partition = partitionStore.getNextPartition();
   *   ... do stuff with partition ...
   *   partitionStore.putPartition(partition);
   *
   * Called from a single thread.
   */
  public abstract void startIteration();

  /**
   * Return the next partition in iteration for the current superstep.
   * Note: user has to put back the partition to the store through
   * {@link #putPartition(Partition)} after use. Look at comments on
   * {@link #startIteration()} for more detail.
   *
   * @return The next partition to process
   */
  public abstract Partition<I, V, E> getNextPartition();

  /**
   * Put a partition back to the store. Use this method to put a partition
   * back after it has been retrieved through {@link #getNextPartition()}.
   * Look at comments on {@link #startIteration()} for more detail.
   *
   * @param partition Partition
   */
  public abstract void putPartition(Partition<I, V, E> partition);

  /**
   * Move edges from edge store to partitions. This method is called from a
   * *single thread* once all vertices and edges are read in INPUT_SUPERSTEP.
   */
  public abstract void moveEdgesToVertices();

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
