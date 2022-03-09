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
package org.apache.giraph.block_app.library;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.PairConsumer;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.FunctionWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utility class for creating sequences of sending replies to received
 * messages. Current instance of this object represents partial chain,
 * where we have specified which messages will be send at the lastly defined
 * link in the chain thus far, but we haven't specified yet what to do when
 * vertices receive them.
 *
 * Contains set of:
 * - static startX methods, used to create the chain
 * - thenX methods, used to add one more Piece to the chain, can be
 *   "chained" arbitrary number of times.
 * - endX methods, used to finish the chain, returning
 *   the Block representing the whole chain
 *
 * If messageSupplier or targetsSupplier returns null, current vertex
 * is not going to send any messages.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <P> Previous value
 */
public class SendMessageChain<I extends WritableComparable, V extends Writable,
    E extends Writable, P> {
  /**
   * Represent current partial chain. Given a way to consume messages
   * received in lastly defined link in this chain, it will produce block
   * representing a chain created thus far.
   */
  private final Function<ConsumerWithVertex<I, V, E, P>, Block> blockCreator;

  private SendMessageChain(
      Function<ConsumerWithVertex<I, V, E, P>, Block> blockCreator) {
    this.blockCreator = blockCreator;
  }

  /**
   * Start chain with sending message provided by messageSupplier to all
   * targets provided by targetsSupplier.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  SendMessageChain<I, V, E, Iterable<M>> startSend(
      final String name,
      final Class<M> messageClass,
      final SupplierFromVertex<I, V, E, M> messageSupplier,
      final SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier) {
    return new SendMessageChain<>(
        new Function<ConsumerWithVertex<I, V, E, Iterable<M>>, Block>() {
          @Override
          public Block apply(
              ConsumerWithVertex<I, V, E, Iterable<M>> messagesConsumer) {
            return Pieces.sendMessage(
                name, messageClass, messageSupplier,
                targetsSupplier, messagesConsumer);
          }
        });
  }

  /**
   * Start chain with sending message provided by messageSupplier to all
   * targets provided by targetsSupplier, and use given messageCombiner to
   * combine messages together.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  SendMessageChain<I, V, E, M> startSend(
      final String name,
      final MessageCombiner<? super I, M> messageCombiner,
      final SupplierFromVertex<I, V, E, M> messageSupplier,
      final SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier) {
    return new SendMessageChain<>(
        new Function<ConsumerWithVertex<I, V, E, M>, Block>() {
          @Override
          public Block apply(ConsumerWithVertex<I, V, E, M> messagesConsumer) {
            return Pieces.sendMessage(
                name, messageCombiner, messageSupplier,
                targetsSupplier, messagesConsumer);
          }
        });
  }

  /**
   * Start chain with sending message provided by messageSupplier to all
   * neighbors of a current vertex.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  SendMessageChain<I, V, E, Iterable<M>> startSendToNeighbors(
      final String name,
      final Class<M> messageClass,
      final SupplierFromVertex<I, V, E, M> messageSupplier) {
    return startSend(name, messageClass, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier());
  }

  /**
   * Start chain with sending message provided by messageSupplier to all
   * neighbors of a current vertex, and use given messageCombiner to
   * combine messages together.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  SendMessageChain<I, V, E, M> startSendToNeighbors(
      final String name,
      final MessageCombiner<? super I, M> messageCombiner,
      final SupplierFromVertex<I, V, E, M> messageSupplier) {
    return startSend(name, messageCombiner, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier());
  }

  /**
   * Start chain by providing a function that will produce Block representing
   * beginning of the chain, given a consumer of messages send
   * by the last link in the created block.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, P extends Writable>
  SendMessageChain<I, V, E, P> startCustom(
      Function<ConsumerWithVertex<I, V, E, P>, Block> createStartingBlock) {
    return new SendMessageChain<>(createStartingBlock);
  }

  /**
   * Give previously received message(s) to messageSupplier, and send message
   * it returns to all targets provided by targetsSupplier.
   */
  public <M extends Writable>
  SendMessageChain<I, V, E, Iterable<M>> thenSend(
      final String name,
      final Class<M> messageClass,
      final FunctionWithVertex<I, V, E, P, M> messageSupplier,
      final SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier) {
    final ObjectTransfer<P> prevMessagesTransfer = new ObjectTransfer<>();

    return new SendMessageChain<>(
        new Function<ConsumerWithVertex<I, V, E, Iterable<M>>, Block>() {
          @Override
          public Block apply(
              ConsumerWithVertex<I, V, E, Iterable<M>> messagesConsumer) {
            return new SequenceBlock(
              blockCreator.apply(
                  prevMessagesTransfer.<I, V, E>castToConsumer()),
              Pieces.sendMessage(
                name, messageClass,
                new SupplierFromVertex<I, V, E, M>() {
                  @Override
                  public M get(Vertex<I, V, E> vertex) {
                    return messageSupplier.apply(
                        vertex, prevMessagesTransfer.get());
                  }
                },
                targetsSupplier, messagesConsumer));
          }
        });
  }

  /**
   * Give previously received message(s) to messageSupplier, and send message
   * it returns to all neighbors of current vertex.
   */
  public <M extends Writable>
  SendMessageChain<I, V, E, Iterable<M>> thenSendToNeighbors(
      final String name,
      final Class<M> messageClass,
      final FunctionWithVertex<I, V, E, P, M> messageSupplier) {
    return thenSend(name, messageClass, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier());
  }

  /**
   * Give previously received message(s) to messageSupplier, and send message
   * it returns to all targets provided by targetsSupplier, and use given
   * messageCombiner to combine messages together.
   */
  public <M extends Writable>
  SendMessageChain<I, V, E, M> thenSend(
      final String name,
      final MessageCombiner<? super I, M> messageCombiner,
      final FunctionWithVertex<I, V, E, P, M> messageSupplier,
      final SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier) {
    final ObjectTransfer<P> prevMessagesTransfer = new ObjectTransfer<>();

    return new SendMessageChain<>(
        new Function<ConsumerWithVertex<I, V, E, M>, Block>() {
          @Override
          public Block apply(ConsumerWithVertex<I, V, E, M> messagesConsumer) {
            return new SequenceBlock(
              blockCreator.apply(
                  prevMessagesTransfer.<I, V, E>castToConsumer()),
              Pieces.sendMessage(
                name, messageCombiner,
                new SupplierFromVertex<I, V, E, M>() {
                  @Override
                  public M get(Vertex<I, V, E> vertex) {
                    return messageSupplier.apply(
                        vertex, prevMessagesTransfer.get());
                  }
                },
                targetsSupplier, messagesConsumer));
          }
        });
  }

  /**
   * Give previously received message(s) to messageSupplier, and send message
   * it returns to all neighbors of current vertex, and use given
   * messageCombiner to combine messages together.
   */
  public <M extends Writable>
  SendMessageChain<I, V, E, M> thenSendToNeighbors(
      final String name,
      final MessageCombiner<? super I, M> messageCombiner,
      final FunctionWithVertex<I, V, E, P, M> messageSupplier) {
    return thenSend(name, messageCombiner, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier());
  }

  /**
   * End chain by giving received messages to valueSupplier,
   * to produce value that should be reduced, and consumed on master
   * by reducedValueConsumer.
   */
  public <S, R extends Writable>
  Block endReduce(final String name, final ReduceOperation<S, R> reduceOp,
      final FunctionWithVertex<I, V, E, P, S> valueSupplier,
      final Consumer<R> reducedValueConsumer) {
    return endCustom(new Function<SupplierFromVertex<I, V, E, P>, Block>() {
      @Override
      public Block apply(final SupplierFromVertex<I, V, E, P> prevMessages) {
        return Pieces.reduce(
            name,
            reduceOp,
            new SupplierFromVertex<I, V, E, S>() {
              @Override
              public S get(Vertex<I, V, E> vertex) {
                return valueSupplier.apply(vertex, prevMessages.get(vertex));
              }
            },
            reducedValueConsumer);
      }
    });
  }

  /**
   * End chain by giving received messages to valueSupplier,
   * to produce value that should be reduced, and consumed on master
   * by reducedValueConsumer.
   */
  public <S, R extends Writable>
  Block endReduceWithMaster(
      final String name, final ReduceOperation<S, R> reduceOp,
      final FunctionWithVertex<I, V, E, P, S> valueSupplier,
      final PairConsumer<R, BlockMasterApi> reducedValueConsumer) {
    return endCustom(new Function<SupplierFromVertex<I, V, E, P>, Block>() {
      @Override
      public Block apply(final SupplierFromVertex<I, V, E, P> prevMessages) {
        return Pieces.reduceWithMaster(
            name,
            reduceOp,
            new SupplierFromVertex<I, V, E, S>() {
              @Override
              public S get(Vertex<I, V, E> vertex) {
                return valueSupplier.apply(vertex, prevMessages.get(vertex));
              }
            },
            reducedValueConsumer);
      }
    });
  }

  /**
   * End chain by processing messages received within the last link
   * in the chain.
   */
  public Block endConsume(ConsumerWithVertex<I, V, E, P> messagesConsumer) {
    return blockCreator.apply(messagesConsumer);
  }

  /**
   * End chain by providing a function that will produce Block to be attached
   * to the end of current chain, given a supplier of messages received
   * within the last link in the chain.
   */
  public Block endCustom(
      Function<SupplierFromVertex<I, V, E, P>, Block> createBlockToAttach) {
    final ObjectTransfer<P> prevMessagesTransfer = new ObjectTransfer<>();
    return new SequenceBlock(
        blockCreator.apply(prevMessagesTransfer.<I, V, E>castToConsumer()),
        createBlockToAttach.apply(
            prevMessagesTransfer.<I, V, E>castToSupplier()));
  }
}
