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
import org.apache.giraph.comm.messages.primitives.IntByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IntFloatMessageStore;
import org.apache.giraph.comm.messages.primitives.LongByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.LongDoubleMessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
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
public class InMemoryMessageStoreFactory<I extends WritableComparable,
    M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, M>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InMemoryMessageStoreFactory.class);

  /** Service worker */
  private final CentralizedServiceWorker<I, ?, ?> service;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

  /**
   * @param service Worker service
   * @param conf    Configuration
   */
  public InMemoryMessageStoreFactory(CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
    this.service = service;
    this.conf = conf;
  }

  @Override
  public MessageStore<I, M> newStore(
      MessageValueFactory<M> messageValueFactory) {
    Class<M> messageClass = messageValueFactory.getValueClass();
    MessageStore messageStore;
    if (conf.useMessageCombiner()) {
      Class<I> vertexIdClass = conf.getVertexIdClass();
      if (vertexIdClass.equals(IntWritable.class) &&
          messageClass.equals(FloatWritable.class)) {
        messageStore = new IntFloatMessageStore(
            (CentralizedServiceWorker<IntWritable, ?, ?>) service,
            (MessageCombiner<IntWritable, FloatWritable>)
                conf.<FloatWritable>createMessageCombiner());
      } else if (vertexIdClass.equals(LongWritable.class) &&
          messageClass.equals(DoubleWritable.class)) {
        messageStore = new LongDoubleMessageStore(
            (CentralizedServiceWorker<LongWritable, ?, ?>) service,
            (MessageCombiner<LongWritable, DoubleWritable>)
                conf.<DoubleWritable>createMessageCombiner());
      } else {
        messageStore = new OneMessagePerVertexStore<I, M>(messageValueFactory,
          service, conf.<M>createMessageCombiner(), conf);
      }
    } else {
      Class<I> vertexIdClass = conf.getVertexIdClass();
      if (vertexIdClass.equals(IntWritable.class)) {
        messageStore = new IntByteArrayMessageStore<M>(messageValueFactory,
            (CentralizedServiceWorker<IntWritable, ?, ?>) service,
            (ImmutableClassesGiraphConfiguration<IntWritable, ?, ?>) conf);
      } else if (vertexIdClass.equals(LongWritable.class)) {
        messageStore = new LongByteArrayMessageStore<M>(messageValueFactory,
            (CentralizedServiceWorker<LongWritable, ?, ?>) service,
            (ImmutableClassesGiraphConfiguration<LongWritable, ?, ?>) conf);
      } else {
        messageStore = new ByteArrayMessagesPerVertexStore<I, M>(
          messageValueFactory, service, conf);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("newStore: Created " + messageStore.getClass() +
          " for vertex id " + conf.getVertexIdClass() +
          " and message value " + messageClass + " and" +
          (conf.useMessageCombiner() ? " message combiner " +
              conf.getMessageCombinerClass() : " no combiner"));
    }
    return (MessageStore<I, M>) messageStore;
  }
}
