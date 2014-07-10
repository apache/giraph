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

package org.apache.giraph.comm.messages.out_of_core;

import com.google.common.collect.Maps;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
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
public class DiskBackedMessageStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    MessageStore<I, M> {
  /** Message value factory */
  private final MessageValueFactory<M> messageValueFactory;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> service;
  /** Number of messages to keep in memory */
  private final int maxNumberOfMessagesInMemory;
  /** Factory for creating file stores when flushing */
  private final MessageStoreFactory<I, M, PartitionDiskBackedMessageStore<I, M>>
  partitionStoreFactory;
  /** Map from partition id to its message store */
  private final ConcurrentMap<Integer, PartitionDiskBackedMessageStore<I, M>>
  partitionMessageStores;

  /**
   * Constructor
   *
   * @param messageValueFactory         Factory for creating message values
   * @param service                     Service worker
   * @param maxNumberOfMessagesInMemory Number of messages to keep in memory
   * @param partitionStoreFactory       Factory for creating stores for a
   *                                    partition
   */
  public DiskBackedMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, V, E> service,
      int maxNumberOfMessagesInMemory,
      MessageStoreFactory<I, M, PartitionDiskBackedMessageStore<I,
          M>> partitionStoreFactory) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.maxNumberOfMessagesInMemory = maxNumberOfMessagesInMemory;
    this.partitionStoreFactory = partitionStoreFactory;
    partitionMessageStores = Maps.newConcurrentMap();
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  @Override
  public void addPartitionMessages(
      int partitionId,
      VertexIdMessages<I, M> messages) throws IOException {
    PartitionDiskBackedMessageStore<I, M> partitionMessageStore =
        getMessageStore(partitionId);
    VertexIdMessageIterator<I, M>
        vertexIdMessageIterator =
        messages.getVertexIdMessageIterator();
    while (vertexIdMessageIterator.hasNext()) {
      vertexIdMessageIterator.next();
      boolean ownsVertexId =
          partitionMessageStore.addVertexMessages(
              vertexIdMessageIterator.getCurrentVertexId(),
              Collections.singleton(
                  vertexIdMessageIterator.getCurrentMessage()));
      if (ownsVertexId) {
        vertexIdMessageIterator.releaseCurrentVertexId();
      }
    }
    checkMemory();
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) throws IOException {
    if (hasMessagesForVertex(vertexId)) {
      return getMessageStore(vertexId).getVertexMessages(vertexId);
    } else {
      return EmptyIterable.get();
    }
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return getMessageStore(vertexId).hasMessagesForVertex(vertexId);
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    PartitionDiskBackedMessageStore<I, M> messageStore =
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
    PartitionDiskBackedMessageStore<I, M> messageStore =
        partitionMessageStores.get(partitionId);
    if (messageStore != null) {
      messageStore.clearAll();
    }
  }

  @Override
  public void clearAll() throws IOException {
    for (PartitionDiskBackedMessageStore<I, M> messageStore :
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
    for (PartitionDiskBackedMessageStore<I, M> messageStore :
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
    PartitionDiskBackedMessageStore<I, M> biggestStore = null;
    for (PartitionDiskBackedMessageStore<I, M> messageStore :
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
  private PartitionDiskBackedMessageStore<I, M> getMessageStore(I vertexId) {
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
  private PartitionDiskBackedMessageStore<I, M> getMessageStore(
      int partitionId) {
    PartitionDiskBackedMessageStore<I, M> messageStore =
        partitionMessageStores.get(partitionId);
    if (messageStore != null) {
      return messageStore;
    }
    messageStore = partitionStoreFactory.newStore(messageValueFactory);
    PartitionDiskBackedMessageStore<I, M> store =
        partitionMessageStores.putIfAbsent(partitionId, messageStore);
    return (store == null) ? messageStore : store;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    PartitionDiskBackedMessageStore<I, M> partitionStore =
        partitionMessageStores.get(partitionId);
    out.writeBoolean(partitionStore != null);
    if (partitionStore != null) {
      partitionStore.write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      PartitionDiskBackedMessageStore<I, M> messageStore =
          partitionStoreFactory.newStore(messageValueFactory);
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
  MessageStoreFactory<I, M, MessageStore<I, M>> newFactory(
    CentralizedServiceWorker<I, V, E> service,
      int maxMessagesInMemory,
      MessageStoreFactory<I, M, PartitionDiskBackedMessageStore<I, M>>
          fileStoreFactory) {
    return new DiskBackedMessageStoreFactory<I, V, E, M>(service,
        maxMessagesInMemory,
        fileStoreFactory);
  }
}

