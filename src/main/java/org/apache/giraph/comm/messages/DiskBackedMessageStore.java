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

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Message storage with in memory map of messages and with support for
 * flushing all the messages to the disk.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class DiskBackedMessageStore<I extends WritableComparable,
    M extends Writable> implements FlushableMessageStore<I, M> {
  /** In memory message map */
  private volatile ConcurrentNavigableMap<I, Collection<M>> inMemoryMessages;
  /** Hadoop configuration */
  private final Configuration config;
  /** Combiner for messages */
  private final VertexCombiner<I, M> combiner;
  /** Counter for number of messages in memory */
  private final AtomicInteger numberOfMessagesInMemory;
  /** To keep vertex ids which we have messages for */
  private final Set<I> destinationVertices;
  /** File stores in which we keep flushed messages */
  private final Collection<BasicMessageStore<I, M>> fileStores;
  /** Factory for creating file stores when flushing */
  private final
  MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;
  /** Lock for disk flushing */
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  /**
   * @param combiner         Combiner for messages
   * @param config           Hadoop configuration
   * @param fileStoreFactory Factory for creating file stores when flushing
   */
  public DiskBackedMessageStore(VertexCombiner<I, M> combiner,
      Configuration config,
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
    inMemoryMessages = new ConcurrentSkipListMap<I, Collection<M>>();
    this.config = config;
    this.combiner = combiner;
    numberOfMessagesInMemory = new AtomicInteger(0);
    destinationVertices =
        Collections.newSetFromMap(Maps.<I, Boolean>newConcurrentMap());
    fileStores = Lists.newArrayList();
    this.fileStoreFactory = fileStoreFactory;
  }

  @Override
  public void addVertexMessages(I vertexId,
      Collection<M> messages) throws IOException {
    destinationVertices.add(vertexId);

    rwLock.readLock().lock();
    try {
      Collection<M> currentMessages =
          CollectionUtils.addConcurrent(vertexId, messages, inMemoryMessages);
      if (combiner != null) {
        synchronized (currentMessages) {
          numberOfMessagesInMemory.addAndGet(
              messages.size() - currentMessages.size());
          currentMessages =
              Lists.newArrayList(combiner.combine(vertexId, currentMessages));
          inMemoryMessages.put(vertexId, currentMessages);
          numberOfMessagesInMemory.addAndGet(currentMessages.size());
        }
      } else {
        numberOfMessagesInMemory.addAndGet(messages.size());
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void addMessages(Map<I, Collection<M>> messages) throws IOException {
    for (Entry<I, Collection<M>> entry : messages.entrySet()) {
      addVertexMessages(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Collection<M> getVertexMessages(I vertexId) throws IOException {
    Collection<M> messages = inMemoryMessages.get(vertexId);
    if (messages == null) {
      messages = Lists.newArrayList();
    }
    for (BasicMessageStore<I, M> fileStore : fileStores) {
      messages.addAll(fileStore.getVertexMessages(vertexId));
    }
    return messages;
  }

  @Override
  public int getNumberOfMessages() {
    return numberOfMessagesInMemory.get();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return destinationVertices.contains(vertexId);
  }

  @Override
  public Iterable<I> getDestinationVertices() {
    return destinationVertices;
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    inMemoryMessages.remove(vertexId);
  }

  @Override
  public void clearAll() throws IOException {
    inMemoryMessages.clear();
    destinationVertices.clear();
    for (BasicMessageStore<I, M> fileStore : fileStores) {
      fileStore.clearAll();
    }
    fileStores.clear();
  }

  @Override
  public void flush() throws IOException {
    ConcurrentNavigableMap<I, Collection<M>> messagesToFlush = null;
    rwLock.writeLock().lock();
    try {
      messagesToFlush = inMemoryMessages;
      inMemoryMessages = new ConcurrentSkipListMap<I, Collection<M>>();
      numberOfMessagesInMemory.set(0);
    } finally {
      rwLock.writeLock().unlock();
    }
    BasicMessageStore<I, M> fileStore = fileStoreFactory.newStore();
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

    // write in memory messages map
    out.writeInt(inMemoryMessages.size());
    for (Entry<I, Collection<M>> entry : inMemoryMessages.entrySet()) {
      entry.getKey().write(out);
      out.writeInt(entry.getValue().size());
      for (M message : entry.getValue()) {
        message.write(out);
      }
    }

    // write file stores
    out.writeInt(fileStores.size());
    for (BasicMessageStore<I, M> fileStore : fileStores) {
      fileStore.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read destination vertices
    int numVertices = in.readInt();
    for (int v = 0; v < numVertices; v++) {
      I vertexId = BspUtils.<I>createVertexId(config);
      vertexId.readFields(in);
      destinationVertices.add(vertexId);
    }

    // read in memory map
    int mapSize = in.readInt();
    for (int m = 0; m < mapSize; m++) {
      I vertexId = BspUtils.<I>createVertexId(config);
      vertexId.readFields(in);
      int numMessages = in.readInt();
      numberOfMessagesInMemory.addAndGet(numMessages);
      List<M> messages = Lists.newArrayList();
      for (int i = 0; i < numMessages; i++) {
        M message = BspUtils.<M>createMessageValue(config);
        message.readFields(in);
        messages.add(message);
      }
      inMemoryMessages.put(vertexId, messages);
    }

    // read file stores
    int numFileStores = in.readInt();
    for (int s = 0; s < numFileStores; s++) {
      BasicMessageStore<I, M> fileStore = fileStoreFactory.newStore();
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
  MessageStoreFactory<I, M, FlushableMessageStore<I, M>> newFactory(
      Configuration config,
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
    return new Factory<I, M>(config, fileStoreFactory);
  }

  /**
   * Factory for {@link DiskBackedMessageStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable> implements MessageStoreFactory<I, M,
      FlushableMessageStore<I, M>> {
    /** Hadoop configuration */
    private final Configuration config;
    /** Combiner for messages */
    private final VertexCombiner<I, M> combiner;
    /** Factory for creating message stores for partitions */
    private final
    MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;

    /**
     * @param config           Hadoop configuration
     * @param fileStoreFactory Factory for creating message stores for
     *                         partitions
     */
    public Factory(Configuration config,
        MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
      this.config = config;
      if (BspUtils.getVertexCombinerClass(config) == null) {
        combiner = null;
      } else {
        combiner = BspUtils.createVertexCombiner(config);
      }
      this.fileStoreFactory = fileStoreFactory;
    }

    @Override
    public FlushableMessageStore<I, M> newStore() {
      return new DiskBackedMessageStore<I, M>(combiner, config,
          fileStoreFactory);
    }
  }
}
