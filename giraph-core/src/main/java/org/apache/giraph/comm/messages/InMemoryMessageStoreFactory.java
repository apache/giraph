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
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.primitives.IdByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IdOneMessagePerVertexStore;
import org.apache.giraph.comm.messages.primitives.IntByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IntFloatMessageStore;
import org.apache.giraph.comm.messages.primitives.LongDoubleMessageStore;
import org.apache.giraph.comm.messages.primitives.long_id.LongByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.long_id.LongPointerListMessageStore;
import org.apache.giraph.comm.messages.queue.AsyncMessageStoreWrapper;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Message store factory which produces message stores which hold all
 * messages in memory. Depending on whether or not combiner is currently used,
 * this factory creates {@link OneMessagePerVertexStore} or
 * {@link ByteArrayMessagesPerVertexStore}
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class InMemoryMessageStoreFactory<I extends WritableComparable,
    M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, M>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InMemoryMessageStoreFactory.class);

  /** Service worker */
  protected CentralizedServiceWorker<I, ?, ?> service;
  /** Hadoop configuration */
  protected ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

  /**
   * Default constructor allowing class invocation via Reflection.
   */
  public InMemoryMessageStoreFactory() {
  }

  /**
   * MessageStore to be used when combiner is enabled
   *
   * @param messageValueFactory message value factory
   * @return message store
   */
  protected MessageStore<I, M> newStoreWithCombiner(
    MessageValueFactory<M> messageValueFactory) {
    Class<M> messageClass = messageValueFactory.getValueClass();
    MessageStore messageStore;
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (vertexIdClass.equals(IntWritable.class) &&
        messageClass.equals(FloatWritable.class)) {
      messageStore = new IntFloatMessageStore(
          (CentralizedServiceWorker<IntWritable, Writable, Writable>) service,
          (MessageCombiner<IntWritable, FloatWritable>)
              conf.<FloatWritable>createMessageCombiner());
    } else if (vertexIdClass.equals(LongWritable.class) &&
        messageClass.equals(DoubleWritable.class)) {
      messageStore = new LongDoubleMessageStore(
          (CentralizedServiceWorker<LongWritable, Writable, Writable>) service,
          (MessageCombiner<LongWritable, DoubleWritable>)
              conf.<DoubleWritable>createMessageCombiner());
    } else {
      PrimitiveIdTypeOps<I> idTypeOps =
          TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(vertexIdClass);
      if (idTypeOps != null) {
        messageStore = new IdOneMessagePerVertexStore<>(
            messageValueFactory, service, conf.<M>createMessageCombiner(),
            conf);
      } else {
        messageStore =
            new OneMessagePerVertexStore<I, M>(messageValueFactory, service,
                conf.<M>createMessageCombiner(), conf);
      }
    }
    return messageStore;
  }

  /**
   * MessageStore to be used when combiner is not enabled
   *
   * @param messageValueFactory message value factory
   * @return message store
   */
  protected MessageStore<I, M> newStoreWithoutCombiner(
    MessageValueFactory<M> messageValueFactory) {
    MessageStore messageStore = null;
    MessageEncodeAndStoreType encodeAndStore = GiraphConstants
        .MESSAGE_ENCODE_AND_STORE_TYPE.get(conf);
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (vertexIdClass.equals(IntWritable.class)) { // INT
      messageStore = new IntByteArrayMessageStore(messageValueFactory,
          service, conf);
    } else if (vertexIdClass.equals(LongWritable.class)) { // LONG
      if (encodeAndStore.equals(
          MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION) ||
          encodeAndStore.equals(
            MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION)) {
        messageStore = new LongByteArrayMessageStore(messageValueFactory,
            service, conf);
      } else if (encodeAndStore.equals(
          MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX)) {
        messageStore = new LongPointerListMessageStore(messageValueFactory,
            service, conf);
      }
    } else { // GENERAL
      if (encodeAndStore.equals(
          MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION) ||
          encodeAndStore.equals(
              MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION)) {
        PrimitiveIdTypeOps<I> idTypeOps =
            TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(vertexIdClass);
        if (idTypeOps != null) {
          messageStore = new IdByteArrayMessageStore<>(
              messageValueFactory, service, conf);
        } else {
          messageStore = new ByteArrayMessagesPerVertexStore<>(
              messageValueFactory, service, conf);
        }
      } else if (encodeAndStore.equals(
          MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX)) {
        messageStore = new PointerListPerVertexStore<>(messageValueFactory,
            service, conf);
      }
    }
    return messageStore;
  }

  @Override
  public MessageStore<I, M> newStore(
      MessageValueFactory<M> messageValueFactory) {
    Class<M> messageClass = messageValueFactory.getValueClass();
    MessageStore messageStore;
    if (conf.useMessageCombiner()) {
      messageStore = newStoreWithCombiner(messageValueFactory);
    } else {
      messageStore = newStoreWithoutCombiner(messageValueFactory);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("newStore: Created " + messageStore.getClass() +
          " for vertex id " + conf.getVertexIdClass() +
          " and message value " + messageClass + " and" +
          (conf.useMessageCombiner() ? " message combiner " +
              conf.getMessageCombinerClass() : " no combiner"));
    }

    int asyncMessageStoreThreads =
        GiraphConstants.ASYNC_MESSAGE_STORE_THREADS_COUNT.get(conf);
    if (asyncMessageStoreThreads > 0) {
      messageStore = new AsyncMessageStoreWrapper(
          messageStore,
          service.getPartitionStore().getPartitionIds(),
          asyncMessageStoreThreads);
    }

    return messageStore;
  }

  @Override
  public void initialize(CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
    this.service = service;
    this.conf = conf;
  }

  @Override
  public boolean shouldTraverseMessagesInOrder() {
    return false;
  }
}
