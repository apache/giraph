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

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.primitives.IdByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IdOneMessagePerVertexStore;
import org.apache.giraph.comm.messages.primitives.IntFloatMessageStore;
import org.apache.giraph.comm.messages.primitives.LongDoubleMessageStore;
import org.apache.giraph.comm.messages.primitives.long_id.LongPointerListPerVertexStore;
import org.apache.giraph.comm.messages.queue.AsyncMessageStoreWrapper;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
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

  /** Partition info */
  protected PartitionSplitInfo<I> partitionInfo;
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
   * @param messageClass message class
   * @param messageValueFactory message value factory
   * @param messageCombiner message combiner
   * @return message store
   */
  protected MessageStore<I, M> newStoreWithCombiner(
      Class<M> messageClass,
      MessageValueFactory<M> messageValueFactory,
      MessageCombiner<? super I, M> messageCombiner) {
    MessageStore messageStore;
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (vertexIdClass.equals(IntWritable.class) &&
        messageClass.equals(FloatWritable.class)) {
      messageStore = new IntFloatMessageStore(
          (PartitionSplitInfo<IntWritable>) partitionInfo,
          (MessageCombiner<IntWritable, FloatWritable>) messageCombiner);
    } else if (vertexIdClass.equals(LongWritable.class) &&
        messageClass.equals(DoubleWritable.class)) {
      messageStore = new LongDoubleMessageStore(
          (PartitionSplitInfo<LongWritable>) partitionInfo,
          (MessageCombiner<LongWritable, DoubleWritable>) messageCombiner);
    } else {
      PrimitiveIdTypeOps<I> idTypeOps =
          TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(vertexIdClass);
      if (idTypeOps != null) {
        messageStore = new IdOneMessagePerVertexStore<>(
          messageValueFactory, partitionInfo, messageCombiner, conf);
      } else {
        messageStore = new OneMessagePerVertexStore<I, M>(
          messageValueFactory, partitionInfo, messageCombiner, conf);
      }
    }
    return messageStore;
  }

  /**
   * MessageStore to be used when combiner is not enabled
   *
   * @param messageClass message class
   * @param messageValueFactory message value factory
   * @param encodeAndStore message encode and store type
   * @return message store
   */
  protected MessageStore<I, M> newStoreWithoutCombiner(
      Class<M> messageClass,
      MessageValueFactory<M> messageValueFactory,
      MessageEncodeAndStoreType encodeAndStore) {
    MessageStore messageStore = null;
    Class<I> vertexIdClass = conf.getVertexIdClass();
    // a special case for LongWritable with POINTER_LIST_PER_VERTEX
    if (vertexIdClass.equals(LongWritable.class) && encodeAndStore.equals(
        MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX)) {
      messageStore = new LongPointerListPerVertexStore(
        messageValueFactory, partitionInfo, conf);
    } else { // GENERAL
      if (encodeAndStore.equals(
          MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION) ||
          encodeAndStore.equals(
              MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION)) {
        PrimitiveIdTypeOps<I> idTypeOps =
            TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(vertexIdClass);
        if (idTypeOps != null) {
          messageStore = new IdByteArrayMessageStore<>(
              messageValueFactory, partitionInfo, conf);
        } else {
          messageStore = new ByteArrayMessagesPerVertexStore<>(
              messageValueFactory, partitionInfo, conf);
        }
      } else if (encodeAndStore.equals(
          MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX)) {
        messageStore = new PointerListPerVertexStore<>(
          messageValueFactory, partitionInfo, conf);
      }
    }
    return messageStore;
  }

  @Override
  public MessageStore<I, M> newStore(
      MessageClasses<I, M> messageClasses) {
    Class<M> messageClass = messageClasses.getMessageClass();
    MessageValueFactory<M> messageValueFactory =
        messageClasses.createMessageValueFactory(conf);
    MessageCombiner<? super I, M> messageCombiner =
        messageClasses.createMessageCombiner(conf);
    MessageStore messageStore;
    if (messageCombiner != null) {
      messageStore = newStoreWithCombiner(
          messageClass, messageValueFactory, messageCombiner);
    } else {
      messageStore = newStoreWithoutCombiner(
          messageClass, messageValueFactory,
          messageClasses.getMessageEncodeAndStoreType());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("newStore: Created " + messageStore.getClass() +
          " for vertex id " + conf.getVertexIdClass() +
          " and message value " + messageClass + " and" +
          (messageCombiner != null ? " message combiner " +
              messageCombiner.getClass() : " no combiner"));
    }

    int asyncMessageStoreThreads =
        GiraphConstants.ASYNC_MESSAGE_STORE_THREADS_COUNT.get(conf);
    if (asyncMessageStoreThreads > 0) {
      messageStore = new AsyncMessageStoreWrapper(
          messageStore,
          partitionInfo.getPartitionIds(),
          asyncMessageStoreThreads);
    }

    return messageStore;
  }

  @Override
  public void initialize(PartitionSplitInfo<I> partitionInfo,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
    this.partitionInfo = partitionInfo;
    this.conf = conf;
  }
}
