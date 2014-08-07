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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Implementation of {@link SimpleMessageStore} where we have a single
 * message per vertex.
 * Used when {@link org.apache.giraph.combiner.MessageCombiner} is provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class OneMessagePerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, M> {
  /** MessageCombiner for messages */
  private final MessageCombiner<I, M> messageCombiner;

  /**
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param messageCombiner MessageCombiner for messages
   * @param config Hadoop configuration
   */
  public OneMessagePerVertexStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      MessageCombiner<I, M> messageCombiner,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, service, config);
    this.messageCombiner =
        messageCombiner;
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  @Override
  public void addPartitionMessages(
      int partitionId,
      VertexIdMessages<I, M> messages) throws IOException {
    ConcurrentMap<I, M> partitionMap =
        getOrCreatePartitionMap(partitionId);
    VertexIdMessageIterator<I, M> vertexIdMessageIterator =
      messages.getVertexIdMessageIterator();
    // This loop is a little complicated as it is optimized to only create
    // the minimal amount of vertex id and message objects as possible.
    while (vertexIdMessageIterator.hasNext()) {
      vertexIdMessageIterator.next();
      I vertexId = vertexIdMessageIterator.getCurrentVertexId();
      M currentMessage =
          partitionMap.get(vertexIdMessageIterator.getCurrentVertexId());
      if (currentMessage == null) {
        M newMessage = messageCombiner.createInitialMessage();
        currentMessage = partitionMap.putIfAbsent(
            vertexIdMessageIterator.releaseCurrentVertexId(), newMessage);
        if (currentMessage == null) {
          currentMessage = newMessage;
        }
      }
      synchronized (currentMessage) {
        messageCombiner.combine(vertexId, currentMessage,
            vertexIdMessageIterator.getCurrentMessage());
      }
    }
  }

  @Override
  protected Iterable<M> getMessagesAsIterable(M message) {
    return Collections.singleton(message);
  }

  @Override
  protected int getNumberOfMessagesIn(ConcurrentMap<I, M> partitionMap) {
    return partitionMap.size();
  }

  @Override
  protected void writeMessages(M messages, DataOutput out) throws IOException {
    messages.write(out);
  }

  @Override
  protected M readFieldsForMessages(DataInput in) throws IOException {
    M message = messageValueFactory.newInstance();
    message.readFields(in);
    return message;
  }


  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, MessageStore<I, M>> newFactory(
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    return new Factory<I, M>(service, config);
  }

  /**
   * Factory for {@link OneMessagePerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable>
      implements MessageStoreFactory<I, M, MessageStore<I, M>> {
    /** Service worker */
    private CentralizedServiceWorker<I, ?, ?> service;
    /** Hadoop configuration */
    private ImmutableClassesGiraphConfiguration<I, ?, ?> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageStore<I, M> newStore(
        MessageValueFactory<M> messageValueFactory) {
      return new OneMessagePerVertexStore<I, M>(messageValueFactory, service,
          config.<M>createMessageCombiner(), config);
    }

    @Override
    public void initialize(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
      this.service = service;
      this.config = conf;
    }

    @Override
    public boolean shouldTraverseMessagesInOrder() {
      return false;
    }
  }
}
