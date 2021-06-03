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
package org.apache.giraph.block_app.framework.api.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.giraph.comm.messages.InMemoryMessageStoreFactory;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Iterators;

/**
 * Interface for internal message store, used by LocalBlockRunner
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
@SuppressWarnings("rawtypes")
interface InternalMessageStore
    <I extends WritableComparable, M extends Writable> {
  Iterator<I> targetVertexIds();
  boolean hasMessage(I id);
  Iterable<M> takeMessages(I id);
  void sendMessage(I id, M message);
  void sendMessageToMultipleEdges(Iterator<I> idIter, M message);
  void finalizeStore();
  Iterable<I> getPartitionDestinationVertices(int partitionId);

  /**
   * A wrapper that uses InMemoryMessageStoreFactory to
   * create MessageStore
   *
   * @param <I> Vertex id type
   * @param <M> Message type
   */
  class InternalWrappedMessageStore
  <I extends WritableComparable, M extends Writable>
  implements InternalMessageStore<I, M> {
    private final MessageStore<I, M> messageStore;
    private final PartitionSplitInfo<I> partitionInfo;

    private InternalWrappedMessageStore(
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
      MessageStore<I, M> messageStore,
      PartitionSplitInfo<I> partitionInfo
    ) {
      this.messageStore = messageStore;
      this.partitionInfo = partitionInfo;
    }

    public static <I extends WritableComparable, M extends Writable>
    InternalMessageStore<I, M> create(
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
      MessageClasses<I, M> messageClasses,
      PartitionSplitInfo<I> partitionInfo
    ) {
      InMemoryMessageStoreFactory<I, M> factory =
        new InMemoryMessageStoreFactory<>();
      factory.initialize(partitionInfo, conf);
      return new InternalWrappedMessageStore<>(
        conf,
        factory.newStore(messageClasses),
        partitionInfo
      );
    }

    @Override
    public void sendMessage(I id, M message) {
      try {
        messageStore.addMessage(id, message);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void sendMessageToMultipleEdges(Iterator<I> idIter, M message) {
      while (idIter.hasNext()) {
        sendMessage(idIter.next(), message);
      }
    }

    @Override
    public Iterable<M> takeMessages(I id) {
      Iterable<M> result = messageStore.getVertexMessages(id);
      messageStore.clearVertexMessages(id);
      return result;
    }

    @Override
    public Iterable<I> getPartitionDestinationVertices(int partitionId) {
      return messageStore.getPartitionDestinationVertices(partitionId);
    }

    @Override
    public Iterator<I> targetVertexIds() {
      List<Iterator<I>> iterators = new ArrayList<>();
      for (int partition : partitionInfo.getPartitionIds()) {
        Iterable<I> vertices =
          messageStore.getPartitionDestinationVertices(partition);
        iterators.add(vertices.iterator());
      }
      return Iterators.concat(iterators.iterator());
    }

    @Override
    public boolean hasMessage(I id) {
      return messageStore.hasMessagesForVertex(id);
    }

    @Override
    public void finalizeStore() {
      messageStore.finalizeStore();
    }
  }

  /**
   * Message store that add checks for whether serialization seems to be
   * working fine
   */
  static class InternalChecksMessageStore
      <I extends WritableComparable, M extends Writable>
      implements InternalMessageStore<I, M> {
    private final InternalMessageStore<I, M> messageStore;
    private final ImmutableClassesGiraphConfiguration<I, ?, ?> conf;
    private final MessageValueFactory<M> messageFactory;

    public InternalChecksMessageStore(
      InternalMessageStore<I, M> messageStore,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
      MessageValueFactory<M> messageFactory
    ) {
      this.messageStore = messageStore;
      this.conf = conf;
      this.messageFactory = messageFactory;
    }

    // Use message copies probabilistically, to catch both not serializing some
    // fields, and storing references from message object itself
    // (which can be reusable).
    private M maybeMessageCopy(M message) {
      M messageCopy = WritableUtils.createCopy(
          message, messageFactory, conf);
      return ThreadLocalRandom.current().nextBoolean() ? messageCopy : message;
    }

    private void checkIdCopy(I id) {
      WritableUtils.createCopy(id, conf.getVertexIdFactory(), conf);
    }

    @Override
    public void sendMessage(I id, M message) {
      checkIdCopy(id);
      messageStore.sendMessage(id, maybeMessageCopy(message));
    }

    @Override
    public void sendMessageToMultipleEdges(
        final Iterator<I> idIter, M message) {
      messageStore.sendMessageToMultipleEdges(
          new Iterator<I>() {
            @Override
            public boolean hasNext() {
              return idIter.hasNext();
            }

            @Override
            public I next() {
              I id = idIter.next();
              checkIdCopy(id);
              return id;
            }

            @Override
            public void remove() {
              idIter.remove();
            }
          },
          maybeMessageCopy(message));
    }

    @Override
    public Iterable<M> takeMessages(I id) {
      checkIdCopy(id);
      return messageStore.takeMessages(id);
    }

    @Override
    public boolean hasMessage(I id) {
      return messageStore.hasMessage(id);
    }

    @Override
    public Iterator<I> targetVertexIds() {
      return messageStore.targetVertexIds();
    }

    @Override
    public Iterable<I> getPartitionDestinationVertices(int partitionId) {
      return messageStore.getPartitionDestinationVertices(partitionId);
    }

    @Override
    public void finalizeStore() {
      messageStore.finalizeStore();
    }
  }
}
