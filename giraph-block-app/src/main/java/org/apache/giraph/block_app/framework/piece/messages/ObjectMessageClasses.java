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
package org.apache.giraph.block_app.framework.piece.messages;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.writable.kryo.KryoWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * MessageClasses implementation that provides factory and combiner instances
 * through a provided supplier.
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
public class ObjectMessageClasses<I extends WritableComparable,
    M extends Writable> extends KryoWritable implements MessageClasses<I, M> {
  private final Class<M> messageClass;
  private final SupplierFromConf<MessageValueFactory<M>>
  messageValueFactorySupplier;
  private final SupplierFromConf<? extends MessageCombiner<? super I, M>>
  messageCombinerSupplier;
  private final MessageEncodeAndStoreType messageEncodeAndStoreType;
  private final boolean ignoreExistingVertices;

  public ObjectMessageClasses() {
    this(null, null, null, null, false);
  }

  public ObjectMessageClasses(Class<M> messageClass,
      SupplierFromConf<MessageValueFactory<M>> messageValueFactorySupplier,
      SupplierFromConf<? extends MessageCombiner<? super I, M>>
        messageCombinerSupplier,
      MessageEncodeAndStoreType messageEncodeAndStoreType,
      boolean ignoreExistingVertices) {
    this.messageClass = messageClass;
    this.messageValueFactorySupplier = messageValueFactorySupplier;
    this.messageCombinerSupplier = messageCombinerSupplier;
    this.messageEncodeAndStoreType = messageEncodeAndStoreType;
    this.ignoreExistingVertices = ignoreExistingVertices;
  }

  @Override
  public Class<M> getMessageClass() {
    return messageClass;
  }

  @Override
  public MessageValueFactory<M> createMessageValueFactory(
      ImmutableClassesGiraphConfiguration conf) {
    return Preconditions.checkNotNull(messageValueFactorySupplier.apply(conf));
  }

  @Override
  public MessageCombiner<? super I, M> createMessageCombiner(
      ImmutableClassesGiraphConfiguration<I, ? extends Writable,
        ? extends Writable> conf) {
    return messageCombinerSupplier != null ?
      Preconditions.checkNotNull(messageCombinerSupplier.apply(conf)) : null;
  }

  @Override
  public boolean useMessageCombiner() {
    return messageCombinerSupplier != null;
  }

  @Override
  public boolean ignoreExistingVertices() {
    return ignoreExistingVertices;
  }

  @Override
  public MessageEncodeAndStoreType getMessageEncodeAndStoreType() {
    return messageEncodeAndStoreType;
  }

  @Override
  public MessageClasses<I, M> createCopyForNewSuperstep() {
    return new ObjectMessageClasses<>(
        messageClass, messageValueFactorySupplier,
        messageCombinerSupplier, messageEncodeAndStoreType,
        ignoreExistingVertices);
  }

  @Override
  public void verifyConsistent(ImmutableClassesGiraphConfiguration conf) {
    MessageValueFactory<M> messageValueFactory =
        messageValueFactorySupplier.apply(conf);
    Preconditions.checkState(
        messageValueFactory.newInstance().getClass().equals(messageClass));

    if (messageCombinerSupplier != null) {
      MessageCombiner<? super I, M> messageCombiner =
          messageCombinerSupplier.apply(conf);
      Preconditions.checkState(messageCombiner.createInitialMessage()
          .getClass().equals(messageClass));
      Class<?>[] combinerTypes = ReflectionUtils.getTypeArguments(
          MessageCombiner.class, messageCombiner.getClass());
      ReflectionUtils.verifyTypes(conf.getVertexIdClass(), combinerTypes[0],
          "Vertex id", messageCombiner.getClass());
      ReflectionUtils.verifyTypes(messageClass, combinerTypes[1],
          "Outgoing message", messageCombiner.getClass());
    }
  }
}
