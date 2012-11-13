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

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link SimpleMessageStore} where we have a collection
 * of messages per vertex.
 * Used when there is no combiner provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class CollectionOfMessagesPerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, Collection<M>> {

  /**
   * Constructor
   *
   * @param service  Service worker
   * @param config   Hadoop configuration
   */
  CollectionOfMessagesPerVertexStore(
      CentralizedServiceWorker<I, ?, ?, M> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
    super(service, config);
  }

  @Override
  protected void addVertexMessagesToPartition(I vertexId,
      Collection<M> messages,
      ConcurrentMap<I, Collection<M>> partitionMap) throws IOException {
    CollectionUtils.addConcurrent(vertexId, messages, partitionMap);
  }

  @Override
  protected void addVertexMessageToPartition(I vertexId, M message,
      ConcurrentMap<I, Collection<M>> partitionMap) throws IOException {
    Collection<M> currentMessages = partitionMap.get(vertexId);
    if (currentMessages == null) {
      Collection<M> newMessages = Lists.newArrayList(message);
      currentMessages = partitionMap.putIfAbsent(vertexId, newMessages);
    }
    // if vertex messages existed before, or putIfAbsent didn't put new list
    if (currentMessages != null) {
      synchronized (currentMessages) {
        currentMessages.add(message);
      }
    }
  }

  @Override
  protected Collection<M> getMessagesAsCollection(Collection<M> messages) {
    return messages;
  }

  @Override
  protected int getNumberOfMessagesIn(
      ConcurrentMap<I, Collection<M>> partitionMap) {
    int numberOfMessages = 0;
    for (Collection<M> messages : partitionMap.values()) {
      numberOfMessages += messages.size();
    }
    return numberOfMessages;
  }

  @Override
  protected void writeMessages(Collection<M> messages,
      DataOutput out) throws IOException {
    out.writeInt(messages.size());
    for (M message : messages) {
      message.write(out);
    }
  }

  @Override
  protected Collection<M> readFieldsForMessages(DataInput in) throws
      IOException {
    int numMessages = in.readInt();
    List<M> messages = Lists.newArrayList();
    for (int m = 0; m < numMessages; m++) {
      M message = config.createMessageValue();
      message.readFields(in);
      messages.add(message);
    }
    return messages;
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
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
      CentralizedServiceWorker<I, ?, ?, M> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
    return new Factory<I, M>(service, config);
  }

  /**
   * Factory for {@link CollectionOfMessagesPerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, ?, ?, M> service;
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?, M> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new CollectionOfMessagesPerVertexStore(service, config);
    }
  }
}
