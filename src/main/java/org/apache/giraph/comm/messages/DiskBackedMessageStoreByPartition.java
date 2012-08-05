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

package org.apache.giraph.comm.messages;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Message store which separates data by partitions,
 * and submits them to underlying message store.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class DiskBackedMessageStoreByPartition<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    MessageStoreByPartition<I, M> {
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /** Number of messages to keep in memory */
  private final int maxNumberOfMessagesInMemory;
  /** Factory for creating file stores when flushing */
  private final
  MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory;
  /** Map from partition id to its message store */
  private final
  ConcurrentMap<Integer, FlushableMessageStore<I, M>> partitionMessageStores;

  /**
   * @param service                     Service worker
   * @param maxNumberOfMessagesInMemory Number of messages to keep in memory
   * @param fileStoreFactory            Factory for creating file stores
   *                                    when flushing
   */
  public DiskBackedMessageStoreByPartition(
      CentralizedServiceWorker<I, V, E, M> service,
      int maxNumberOfMessagesInMemory,
      MessageStoreFactory<I, M, FlushableMessageStore<I,
          M>> fileStoreFactory) {
    this.service = service;
    this.maxNumberOfMessagesInMemory = maxNumberOfMessagesInMemory;
    this.fileStoreFactory = fileStoreFactory;
    partitionMessageStores = Maps.newConcurrentMap();
  }

  @Override
  public void addVertexMessages(I vertexId,
      Collection<M> messages) throws IOException {
    getMessageStore(vertexId).addVertexMessages(vertexId, messages);
    checkMemory();
  }

  @Override
  public void addMessages(Map<I, Collection<M>> messages) throws IOException {
    for (Entry<I, Collection<M>> entry : messages.entrySet()) {
      getMessageStore(entry.getKey()).addVertexMessages(
          entry.getKey(), entry.getValue());
    }
    checkMemory();
  }

  @Override
  public void addPartitionMessages(Map<I, Collection<M>> messages,
      int partitionId) throws IOException {
    getMessageStore(partitionId).addMessages(messages);
    checkMemory();
  }

  @Override
  public Collection<M> getVertexMessages(I vertexId) throws IOException {
    if (hasMessagesForVertex(vertexId)) {
      return getMessageStore(vertexId).getVertexMessages(vertexId);
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public int getNumberOfMessages() {
    int numOfMessages = 0;
    for (FlushableMessageStore<I, M> messageStore :
        partitionMessageStores.values()) {
      numOfMessages += messageStore.getNumberOfMessages();
    }
    return numOfMessages;
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return getMessageStore(vertexId).hasMessagesForVertex(vertexId);
  }

  @Override
  public Iterable<I> getDestinationVertices() {
    List<I> vertices = Lists.newArrayList();
    for (FlushableMessageStore<I, M> messageStore :
        partitionMessageStores.values()) {
      Iterables.addAll(vertices, messageStore.getDestinationVertices());
    }
    return vertices;
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    FlushableMessageStore<I, M> messageStore =
        partitionMessageStores.get(partitionId);
    if (messageStore == null) {
      return Collections.emptyList();
    } else {
      return messageStore.getDestinationVertices();
    }
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    if (hasMessagesForVertex(vertexId)) {
      getMessageStore(vertexId).clearVertexMessages(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    FlushableMessageStore<I, M> messageStore =
        partitionMessageStores.get(partitionId);
    if (messageStore != null) {
      messageStore.clearAll();
    }
  }

  @Override
  public void clearAll() throws IOException {
    for (FlushableMessageStore<I, M> messageStore :
        partitionMessageStores.values()) {
      messageStore.clearAll();
    }
    partitionMessageStores.clear();
  }

  /**
   * Checks the memory status, flushes if necessary
   *
   * @throws IOException
   */
  private void checkMemory() throws IOException {
    while (memoryFull()) {
      flushOnePartition();
    }
  }

  /**
   * Check if memory is full
   *
   * @return True iff memory is full
   */
  private boolean memoryFull() {
    int totalMessages = 0;
    for (FlushableMessageStore<I, M> messageStore :
        partitionMessageStores.values()) {
      totalMessages += messageStore.getNumberOfMessages();
    }
    return totalMessages > maxNumberOfMessagesInMemory;
  }

  /**
   * Finds biggest partition and flushes it to the disk
   *
   * @throws IOException
   */
  private void flushOnePartition() throws IOException {
    int maxMessages = 0;
    FlushableMessageStore<I, M> biggestStore = null;
    for (FlushableMessageStore<I, M> messageStore :
        partitionMessageStores.values()) {
      int numMessages = messageStore.getNumberOfMessages();
      if (numMessages > maxMessages) {
        maxMessages = numMessages;
        biggestStore = messageStore;
      }
    }
    if (biggestStore != null) {
      biggestStore.flush();
    }
  }

  /**
   * Get message store for partition which holds vertex with required vertex
   * id
   *
   * @param vertexId Id of vertex for which we are asking for message store
   * @return Requested message store
   */
  private FlushableMessageStore<I, M> getMessageStore(I vertexId) {
    int partitionId =
        service.getVertexPartitionOwner(vertexId).getPartitionId();
    return getMessageStore(partitionId);
  }

  /**
   * Get message store for partition id. It it doesn't exist yet,
   * creates a new one.
   *
   * @param partitionId Id of partition for which we are asking for message
   *                    store
   * @return Requested message store
   */
  private FlushableMessageStore<I, M> getMessageStore(int partitionId) {
    FlushableMessageStore<I, M> messageStore =
        partitionMessageStores.get(partitionId);
    if (messageStore != null) {
      return messageStore;
    }
    messageStore = fileStoreFactory.newStore();
    FlushableMessageStore<I, M> store =
        partitionMessageStores.putIfAbsent(partitionId, messageStore);
    return (store == null) ? messageStore : store;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    FlushableMessageStore<I, M> partitionStore =
        partitionMessageStores.get(partitionId);
    out.writeBoolean(partitionStore != null);
    if (partitionStore != null) {
      partitionStore.write(out);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionMessageStores.size());
    for (Entry<Integer, FlushableMessageStore<I, M>> entry :
        partitionMessageStores.entrySet()) {
      out.writeInt(entry.getKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      FlushableMessageStore<I, M> messageStore = fileStoreFactory.newStore();
      messageStore.readFields(in);
      partitionMessageStores.put(partitionId, messageStore);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numStores = in.readInt();
    for (int s = 0; s < numStores; s++) {
      int partitionId = in.readInt();
      FlushableMessageStore<I, M> messageStore = fileStoreFactory.newStore();
      messageStore.readFields(in);
      partitionMessageStores.put(partitionId, messageStore);
    }
  }


  /**
   * Create new factory for this message store
   *
   * @param service             Service worker
   * @param maxMessagesInMemory Number of messages to keep in memory
   * @param fileStoreFactory    Factory for creating file stores when
   *                            flushing
   * @param <I>                 Vertex id
   * @param <V>                 Vertex data
   * @param <E>                 Edge data
   * @param <M>                 Message data
   * @return Factory
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, M extends Writable>
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
      CentralizedServiceWorker<I, V, E, M> service,
      int maxMessagesInMemory,
      MessageStoreFactory<I, M, FlushableMessageStore<I, M>>
          fileStoreFactory) {
    return new Factory<I, V, E, M>(service, maxMessagesInMemory,
        fileStoreFactory);
  }

  /**
   * Factory for {@link DiskBackedMessageStoreByPartition}
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      V extends Writable, E extends Writable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, V, E, M> service;
    /** Number of messages to keep in memory */
    private final int maxMessagesInMemory;
    /** Factory for creating file stores when flushing */
    private final
    MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory;

    /**
     * @param service             Service worker
     * @param maxMessagesInMemory Number of messages to keep in memory
     * @param fileStoreFactory    Factory for creating file stores when
     *                            flushing
     */
    public Factory(CentralizedServiceWorker<I, V, E, M> service,
        int maxMessagesInMemory,
        MessageStoreFactory<I, M, FlushableMessageStore<I, M>>
            fileStoreFactory) {
      this.service = service;
      this.maxMessagesInMemory = maxMessagesInMemory;
      this.fileStoreFactory = fileStoreFactory;
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new DiskBackedMessageStoreByPartition<I, V, E, M>(service,
          maxMessagesInMemory, fileStoreFactory);
    }
  }

}
