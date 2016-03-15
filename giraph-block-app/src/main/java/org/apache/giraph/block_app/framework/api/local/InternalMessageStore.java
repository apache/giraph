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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.types.ops.TypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.AbstractIterator;

/**
 * Interface for internal message store, used by LocalBlockRunner
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
@SuppressWarnings("rawtypes")
interface InternalMessageStore
    <I extends WritableComparable, M extends Writable> {
  Set<I> targetsSet();
  Iterable<M> takeMessages(I id);
  void sendMessage(I id, M message);
  void sendMessageToMultipleEdges(Iterator<I> idIter, M message);

  /**
   * Abstract Internal message store implementation that uses
   * ConcurrentHashMap to store objects received thus far.
   *
   * @param <I> Vertex id type
   * @param <M> Message type
   * @param <R> Receiver object that particular implementation uses
   *            (message, array of messages, byte array, etc)
   */
  abstract class InternalConcurrentMessageStore
      <I extends WritableComparable, M extends Writable, R>
      implements InternalMessageStore<I, M> {
    private final ConcurrentHashMap<I, R> received =
        new ConcurrentHashMap<>();

    private final Class<I> idClass;
    private final TypeOps<I> idTypeOps;

    InternalConcurrentMessageStore(Class<I> idClass) {
      this.idClass = idClass;
      idTypeOps = TypeOpsUtils.getTypeOpsOrNull(idClass);
    }

    public I copyId(I id) {
      if (idTypeOps != null) {
        return idTypeOps.createCopy(id);
      } else {
        return WritableUtils.createCopy(id, idClass, null);
      }
    }

    R getReceiverFor(I id) {
      R value = received.get(id);

      if (value == null) {
        id = copyId(id);
        value = createNewReceiver();
        R oldValue = received.putIfAbsent(id, value);
        if (oldValue != null) {
          value = oldValue;
        }
      }
      return value;
    }

    R removeFor(I id) {
      return received.remove(id);
    }

    abstract R createNewReceiver();

    @Override
    public Set<I> targetsSet() {
      return received.keySet();
    }

    @Override
    public void sendMessageToMultipleEdges(Iterator<I> idIter, M message) {
      while (idIter.hasNext()) {
        sendMessage(idIter.next(), message);
      }
    }

    public static <I extends WritableComparable, M extends Writable>
    InternalMessageStore<I, M> createMessageStore(
      final ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
      final MessageClasses<I, M> messageClasses
    ) {
      MessageCombiner<? super I, M> combiner =
          messageClasses.createMessageCombiner(conf);
      if (combiner != null) {
        return new InternalCombinerMessageStore<>(
            conf.getVertexIdClass(), combiner);
      } else if (messageClasses.getMessageEncodeAndStoreType().equals(
          MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX)) {
        return new InternalSharedByteMessageStore<>(
            conf.getVertexIdClass(),
            messageClasses.createMessageValueFactory(conf));
      } else {
        return new InternalByteMessageStore<>(
          conf.getVertexIdClass(),
          messageClasses.createMessageValueFactory(conf),
          conf);
      }
    }

    public static <I extends WritableComparable, M extends Writable>
    InternalMessageStore<I, M> createMessageStore(
        final ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
        final BlockWorkerPieces pieces, boolean runAllChecks) {
      @SuppressWarnings("unchecked")
      MessageClasses<I, M> messageClasses =
          pieces.getOutgoingMessageClasses(conf);

      InternalMessageStore<I, M> messageStore =
          createMessageStore(conf, messageClasses);
      if (runAllChecks) {
        return new InternalChecksMessageStore<I, M>(
            messageStore, conf, messageClasses.createMessageValueFactory(conf));
      } else {
        return messageStore;
      }
    }
  }

  /**
   * InternalMessageStore that combines messages as they are received.
   *
   * @param <I> Vertex id value type
   * @param <M> Message type
   */
  static class InternalCombinerMessageStore
      <I extends WritableComparable, M extends Writable>
      extends InternalConcurrentMessageStore<I, M, M> {
    private final MessageCombiner<? super I, M> messageCombiner;

    public InternalCombinerMessageStore(Class<I> idClass,
        MessageCombiner<? super I, M> messageCombiner) {
      super(idClass);
      this.messageCombiner = messageCombiner;
    }

    @Override
    public Iterable<M> takeMessages(I id) {
      M message = removeFor(id);
      if (message != null) {
        return Collections.singleton(message);
      } else {
        return null;
      }
    }

    @Override
    public void sendMessage(I id, M message) {
      M mainMessage = getReceiverFor(id);
      synchronized (mainMessage) {
        messageCombiner.combine(id, mainMessage, message);
      }
    }

    @Override
    M createNewReceiver() {
      return messageCombiner.createInitialMessage();
    }
  }

  /**
   * InternalMessageStore that keeps messages for each vertex in byte array.
   *
   * @param <I> Vertex id value type
   * @param <M> Message type
   */
  static class InternalByteMessageStore
      <I extends WritableComparable, M extends Writable>
      extends InternalConcurrentMessageStore<I, M,
          ExtendedDataOutput> {
    private final MessageValueFactory<M> messageFactory;
    private final ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

    public InternalByteMessageStore(
      Class<I> idClass, MessageValueFactory<M> messageFactory,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf
    ) {
      super(idClass);
      this.messageFactory = messageFactory;
      this.conf = conf;
    }

    @Override
    public Iterable<M> takeMessages(I id) {
      final ExtendedDataOutput out = removeFor(id);
      if (out == null) {
        return null;
      }

      return new Iterable<M>() {
        @Override
        public Iterator<M> iterator() {
          final ExtendedDataInput in = conf.createExtendedDataInput(
            out.getByteArray(), 0, out.getPos()
          );

          final M message = messageFactory.newInstance();
          return new AbstractIterator<M>() {
            @Override
            protected M computeNext() {
              if (in.available() == 0) {
                return endOfData();
              }
              try {
                message.readFields(in);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return message;
            }
          };
        }
      };
    }

    @Override
    public void sendMessage(I id, M message) {
      ExtendedDataOutput out = getReceiverFor(id);

      synchronized (out) {
        try {
          message.write(out);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    ExtendedDataOutput createNewReceiver() {
      return conf.createExtendedDataOutput();
    }
  }

  /**
   * InternalMessageStore that creates byte[] for each message, and
   * all receivers share the same byte[].
   *
   * @param <I> Vertex id value type
   * @param <M> Message type
   */
  static class InternalSharedByteMessageStore
      <I extends WritableComparable, M extends Writable>
      extends InternalConcurrentMessageStore<I, M, List<byte[]>> {
    private final MessageValueFactory<M> messageFactory;

    public InternalSharedByteMessageStore(
        Class<I> idClass, MessageValueFactory<M> messageFactory) {
      super(idClass);
      this.messageFactory = messageFactory;
    }

    @Override
    public Iterable<M> takeMessages(I id) {
      final List<byte[]> out = removeFor(id);
      if (out == null) {
        return null;
      }

      return new Iterable<M>() {
        @Override
        public Iterator<M> iterator() {
          final Iterator<byte[]> byteIter = out.iterator();
          final M message = messageFactory.newInstance();
          final UnsafeReusableByteArrayInput reusableInput =
              new UnsafeReusableByteArrayInput();

          return new Iterator<M>() {
            @Override
            public boolean hasNext() {
              return byteIter.hasNext();
            }

            @Override
            public M next() {
              WritableUtils.fromByteArrayUnsafe(
                  byteIter.next(), message, reusableInput);
              return message;
            }

            @Override
            public void remove() {
              byteIter.remove();
            }
          };
        }
      };
    }

    private void storeMessage(I id, byte[] messageData) {
      List<byte[]> out = getReceiverFor(id);
      synchronized (out) {
        out.add(messageData);
      }
    }

    @Override
    List<byte[]> createNewReceiver() {
      return new ArrayList<>();
    }

    @Override
    public void sendMessage(I id, M message) {
      storeMessage(id, WritableUtils.toByteArrayUnsafe(message));
    }

    @Override
    public void sendMessageToMultipleEdges(Iterator<I> idIter, M message) {
      byte[] messageData = WritableUtils.toByteArrayUnsafe(message);
      while (idIter.hasNext()) {
        storeMessage(idIter.next(), messageData);
      }
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

    public InternalChecksMessageStore(InternalMessageStore<I, M> messageStore,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf,
        MessageValueFactory<M> messageFactory) {
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
    public Set<I> targetsSet() {
      return messageStore.targetsSet();
    }
  }
}
