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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message storage with in-memory map of messages and with support for
 * flushing all the messages to the disk.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class DiskBackedMessageStore<I extends WritableComparable,
    M extends Writable> implements FlushableMessageStore<I, M> {
  /**
   * In-memory message map (must be sorted to insure that the ids are
   * ordered)
   */
  private volatile ConcurrentNavigableMap<I, ExtendedDataOutput>
  inMemoryMessages;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;
  /** Counter for number of messages in-memory */
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
   * Constructor.
   *
   * @param config           Hadoop configuration
   * @param fileStoreFactory Factory for creating file stores when flushing
   */
  public DiskBackedMessageStore(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
    inMemoryMessages = new ConcurrentSkipListMap<I, ExtendedDataOutput>();
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
      ExtendedDataOutput extendedDataOutput = inMemoryMessages.get(vertexId);
      if (extendedDataOutput == null) {
        ExtendedDataOutput newExtendedDataOutput =
            config.createExtendedDataOutput();
        extendedDataOutput =
            inMemoryMessages.putIfAbsent(vertexId, newExtendedDataOutput);
        if (extendedDataOutput == null) {
          ownsVertexId = true;
          extendedDataOutput = newExtendedDataOutput;
        }
      }

      synchronized (extendedDataOutput) {
        for (M message : messages) {
          message.write(extendedDataOutput);
          numberOfMessagesInMemory.getAndIncrement();
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }

    return ownsVertexId;
  }

  @Override
  public void addMessages(MessageStore<I, M> messageStore) throws
      IOException {
    for (I destinationVertex : messageStore.getDestinationVertices()) {
      addVertexMessages(destinationVertex,
          messageStore.getVertexMessages(destinationVertex));
    }
  }

  /**
   * Special iterable that recycles the message
   */
  private class MessageIterable extends RepresentativeByteArrayIterable<M> {
    /**
     * Constructor
     *
     * @param buf Buffer
     * @param off Offset to start in the buffer
     * @param length Length of the buffer
     */
    public MessageIterable(
        byte[] buf, int off, int length) {
      super(config, buf, off, length);
    }

    @Override
    protected M createWritable() {
      return config.createMessageValue();
    }
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) throws IOException {
    ExtendedDataOutput extendedDataOutput = inMemoryMessages.get(vertexId);
    if (extendedDataOutput == null) {
      extendedDataOutput = config.createExtendedDataOutput();
    }
    Iterable<M> combinedIterable = new MessageIterable(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());

    for (BasicMessageStore<I, M> fileStore : fileStores) {
      combinedIterable = Iterables.concat(combinedIterable,
          fileStore.getVertexMessages(vertexId));
    }
    return combinedIterable;
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

  /**
   * Special temporary message store for passing along in-memory messages
   */
  private class TemporaryMessageStore implements MessageStore<I, M> {
    /**
     * In-memory message map (must be sorted to insure that the ids are
     * ordered)
     */
    private final ConcurrentNavigableMap<I, ExtendedDataOutput>
    temporaryMessages;

    /**
     * Constructor.
     *
     * @param temporaryMessages Messages to be owned by this object
     */
    private TemporaryMessageStore(
        ConcurrentNavigableMap<I, ExtendedDataOutput>
            temporaryMessages) {
      this.temporaryMessages = temporaryMessages;
    }

    @Override
    public int getNumberOfMessages() {
      throw new IllegalAccessError("getNumberOfMessages: Not supported");
    }

    @Override
    public boolean hasMessagesForVertex(I vertexId) {
      return temporaryMessages.containsKey(vertexId);
    }

    @Override
    public Iterable<I> getDestinationVertices() {
      return temporaryMessages.keySet();
    }

    @Override
    public void addMessages(MessageStore<I, M> messageStore)
      throws IOException {
      throw new IllegalAccessError("addMessages: Not supported");
    }

    @Override
    public Iterable<M> getVertexMessages(I vertexId) throws IOException {
      ExtendedDataOutput extendedDataOutput = temporaryMessages.get(vertexId);
      if (extendedDataOutput == null) {
        extendedDataOutput = config.createExtendedDataOutput();
      }
      return new MessageIterable(extendedDataOutput.getByteArray(), 0,
          extendedDataOutput.getPos());
    }

    @Override
    public void clearVertexMessages(I vertexId) throws IOException {
      temporaryMessages.remove(vertexId);
    }

    @Override
    public void clearAll() throws IOException {
      temporaryMessages.clear();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new IllegalAccessError("write: Not supported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new IllegalAccessError("readFields: Not supported");
    }
  }

  @Override
  public void flush() throws IOException {
    ConcurrentNavigableMap<I, ExtendedDataOutput> messagesToFlush = null;
    rwLock.writeLock().lock();
    try {
      messagesToFlush = inMemoryMessages;
      inMemoryMessages = new ConcurrentSkipListMap<I, ExtendedDataOutput>();
      numberOfMessagesInMemory.set(0);
    } finally {
      rwLock.writeLock().unlock();
    }
    BasicMessageStore<I, M> fileStore = fileStoreFactory.newStore();
    fileStore.addMessages(new TemporaryMessageStore(messagesToFlush));

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
    for (Entry<I, ExtendedDataOutput> entry : inMemoryMessages.entrySet()) {
      entry.getKey().write(out);
      out.writeInt(entry.getValue().getPos());
      out.write(entry.getValue().getByteArray(), 0, entry.getValue().getPos());
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
      I vertexId = (I) config.createVertexId();
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
      int messageBytes = in.readInt();
      byte[] buf = new byte[messageBytes];
      ExtendedDataOutput extendedDataOutput =
          config.createExtendedDataOutput(buf, messageBytes);
      inMemoryMessages.put(vertexId, extendedDataOutput);
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
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
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
    private final ImmutableClassesGiraphConfiguration config;
    /** Factory for creating message stores for partitions */
    private final
    MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;

    /**
     * @param config           Hadoop configuration
     * @param fileStoreFactory Factory for creating message stores for
     *                         partitions
     */
    public Factory(ImmutableClassesGiraphConfiguration config,
        MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
      this.config = config;
      this.fileStoreFactory = fileStoreFactory;
    }

    @Override
    public FlushableMessageStore<I, M> newStore() {
      return new DiskBackedMessageStore<I, M>(config, fileStoreFactory);
    }
  }
}
