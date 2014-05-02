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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Message storage with in-memory map of messages and with support for
 * flushing all the messages to the disk. Holds messages for a single partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class PartitionDiskBackedMessageStore<I extends WritableComparable,
    M extends Writable> implements Writable {
  /** Message value factory */
  private final MessageValueFactory<M> messageValueFactory;
  /**
   * In-memory message map (must be sorted to insure that the ids are
   * ordered)
   */
  private volatile ConcurrentNavigableMap<I, DataInputOutput>
  inMemoryMessages;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;
  /** Counter for number of messages in-memory */
  private final AtomicInteger numberOfMessagesInMemory;
  /** To keep vertex ids which we have messages for */
  private final Set<I> destinationVertices;
  /** File stores in which we keep flushed messages */
  private final Collection<SequentialFileMessageStore<I, M>> fileStores;
  /** Factory for creating file stores when flushing */
  private final
  MessageStoreFactory<I, M, SequentialFileMessageStore<I, M>> fileStoreFactory;
  /** Lock for disk flushing */
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  /**
   * Constructor.
   *
   * @param messageValueFactory Used to create message values
   * @param config              Hadoop configuration
   * @param fileStoreFactory    Factory for creating file stores when flushing
   */
  public PartitionDiskBackedMessageStore(
      MessageValueFactory<M> messageValueFactory,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config,
      MessageStoreFactory<I, M, SequentialFileMessageStore<I, M>>
          fileStoreFactory) {
    inMemoryMessages = new ConcurrentSkipListMap<I, DataInputOutput>();
    this.messageValueFactory = messageValueFactory;
    this.config = config;
    numberOfMessagesInMemory = new AtomicInteger(0);
    destinationVertices =
        Collections.newSetFromMap(Maps.<I, Boolean>newConcurrentMap());
    fileStores = Lists.newArrayList();
    this.fileStoreFactory = fileStoreFactory;
  }

  /**
   * Add vertex messages
   *
   * @param vertexId Vertex id to use
   * @param messages Messages to add (note that the lifetime of the messages)
   *                 is only until next() is called again)
   * @return True if the vertex id ownership is taken by this method,
   *         false otherwise
   * @throws IOException
   */
  boolean addVertexMessages(I vertexId,
                            Iterable<M> messages) throws IOException {
    boolean ownsVertexId = false;
    destinationVertices.add(vertexId);
    rwLock.readLock().lock();
    try {
      DataInputOutput dataInputOutput = inMemoryMessages.get(vertexId);
      if (dataInputOutput == null) {
        DataInputOutput newDataInputOutput =
            config.createMessagesInputOutput();
        dataInputOutput =
            inMemoryMessages.putIfAbsent(vertexId, newDataInputOutput);
        if (dataInputOutput == null) {
          ownsVertexId = true;
          dataInputOutput = newDataInputOutput;
        }
      }

      synchronized (dataInputOutput) {
        for (M message : messages) {
          message.write(dataInputOutput.getDataOutput());
          numberOfMessagesInMemory.getAndIncrement();
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }

    return ownsVertexId;
  }

  /**
   * Get the messages for a vertex.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Iterable of messages for a vertex id
   */
  public Iterable<M> getVertexMessages(I vertexId) throws IOException {
    DataInputOutput dataInputOutput = inMemoryMessages.get(vertexId);
    if (dataInputOutput == null) {
      dataInputOutput = config.createMessagesInputOutput();
    }
    Iterable<M> combinedIterable = new MessagesIterable<M>(
        dataInputOutput, messageValueFactory);

    for (SequentialFileMessageStore<I, M> fileStore : fileStores) {
      combinedIterable = Iterables.concat(combinedIterable,
          fileStore.getVertexMessages(vertexId));
    }
    return combinedIterable;
  }

  /**
   * Get number of messages in memory
   *
   * @return Number of messages in memory
   */
  public int getNumberOfMessages() {
    return numberOfMessagesInMemory.get();
  }

  /**
   * Check if we have messages for some vertex
   *
   * @param vertexId Id of vertex which we want to check
   * @return True iff we have messages for vertex with required id
   */
  public boolean hasMessagesForVertex(I vertexId) {
    return destinationVertices.contains(vertexId);
  }

  /**
   * Gets vertex ids which we have messages for
   *
   * @return Iterable over vertex ids which we have messages for
   */
  public Iterable<I> getDestinationVertices() {
    return destinationVertices;
  }

  /**
   * Clears messages for a vertex.
   *
   * @param vertexId Vertex id for which we want to clear messages
   * @throws IOException
   */
  public void clearVertexMessages(I vertexId) throws IOException {
    inMemoryMessages.remove(vertexId);
  }

  /**
   * Clears all resources used by this store.
   *
   * @throws IOException
   */
  public void clearAll() throws IOException {
    inMemoryMessages.clear();
    destinationVertices.clear();
    for (SequentialFileMessageStore<I, M> fileStore : fileStores) {
      fileStore.clearAll();
    }
    fileStores.clear();
  }

  /**
   * Flushes messages to the disk.
   *
   * @throws IOException
   */
  public void flush() throws IOException {
    ConcurrentNavigableMap<I, DataInputOutput> messagesToFlush = null;
    rwLock.writeLock().lock();
    try {
      messagesToFlush = inMemoryMessages;
      inMemoryMessages = new ConcurrentSkipListMap<I, DataInputOutput>();
      numberOfMessagesInMemory.set(0);
    } finally {
      rwLock.writeLock().unlock();
    }
    SequentialFileMessageStore<I, M> fileStore =
        fileStoreFactory.newStore(messageValueFactory);
    fileStore.addMessages(messagesToFlush);

    synchronized (fileStores) {
      fileStores.add(fileStore);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write destination vertices
    out.writeInt(destinationVertices.size());
    for (I vertexId : destinationVertices) {
      vertexId.write(out);
    }

    // write of in-memory messages
    out.writeInt(numberOfMessagesInMemory.get());

    // write in-memory messages map
    out.writeInt(inMemoryMessages.size());
    for (Entry<I, DataInputOutput> entry : inMemoryMessages.entrySet()) {
      entry.getKey().write(out);
      entry.getValue().write(out);
    }

    // write file stores
    out.writeInt(fileStores.size());
    for (SequentialFileMessageStore<I, M> fileStore : fileStores) {
      fileStore.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read destination vertices
    int numVertices = in.readInt();
    for (int v = 0; v < numVertices; v++) {
      I vertexId = config.createVertexId();
      vertexId.readFields(in);
      destinationVertices.add(vertexId);
    }

    // read in-memory messages
    numberOfMessagesInMemory.set(in.readInt());

    // read in-memory map
    int mapSize = in.readInt();
    for (int m = 0; m < mapSize; m++) {
      I vertexId = config.createVertexId();
      vertexId.readFields(in);
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      inMemoryMessages.put(vertexId, dataInputOutput);
    }

    // read file stores
    int numFileStores = in.readInt();
    for (int s = 0; s < numFileStores; s++) {
      SequentialFileMessageStore<I, M> fileStore =
          fileStoreFactory.newStore(messageValueFactory);
      fileStore.readFields(in);
      fileStores.add(fileStore);
    }
  }


  /**
   * Create new factory for this message store
   *
   * @param config           Hadoop configuration
   * @param fileStoreFactory Factory for creating message stores for
   *                         partitions
   * @param <I>              Vertex id
   * @param <M>              Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, PartitionDiskBackedMessageStore<I, M>> newFactory(
      ImmutableClassesGiraphConfiguration<I, ?, ?> config,
      MessageStoreFactory<I, M, SequentialFileMessageStore<I, M>>
          fileStoreFactory) {
    return new Factory<I, M>(config, fileStoreFactory);
  }

  /**
   * Factory for {@link PartitionDiskBackedMessageStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable> implements MessageStoreFactory<I, M,
      PartitionDiskBackedMessageStore<I, M>> {
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;
    /** Factory for creating message stores for partitions */
    private final MessageStoreFactory<I, M, SequentialFileMessageStore<I, M>>
    fileStoreFactory;

    /**
     * @param config           Hadoop configuration
     * @param fileStoreFactory Factory for creating message stores for
     *                         partitions
     */
    public Factory(ImmutableClassesGiraphConfiguration<I, ?, ?> config,
        MessageStoreFactory<I, M, SequentialFileMessageStore<I, M>>
            fileStoreFactory) {
      this.config = config;
      this.fileStoreFactory = fileStoreFactory;
    }

    @Override
    public PartitionDiskBackedMessageStore<I, M> newStore(
        MessageValueFactory<M> messageValueFactory) {
      return new PartitionDiskBackedMessageStore<I, M>(messageValueFactory,
          config, fileStoreFactory);
    }

    @Override
    public void initialize(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
      /* Implementation of this method is required if the class is to
       * be exposed publicly and allow instantiating the class via the
       * configuration parameter MESSAGE_STORE_FACTORY_CLASS. As this is
       * a private class, hence the implementation of this method is skipped
       * as the caller knows the specific required constructor parameters
       * for instantiation.
      */
    }

    @Override
    public boolean shouldTraverseMessagesInOrder() {
      return true;
    }
  }
}
