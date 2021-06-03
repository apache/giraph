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
package org.apache.giraph.block_app.library.internal;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.delegate.FilteringPiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.striping.StripingUtils;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Piece that sends a message provided through messageProducer to given set of
 * neighbors, uses a message combiner and passes them to messagesConsumer.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 */
public class SendMessageWithCombinerPiece<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends Piece<I, V, E, M, Object> {
  private final String name;
  private final MessageCombiner<? super I, M> messageCombiner;
  private final SupplierFromVertex<I, V, E, M> messageSupplier;
  private final SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier;
  private final ConsumerWithVertex<I, V, E, M> messagesConsumer;

  public SendMessageWithCombinerPiece(String name,
      MessageCombiner<? super I, M> messageCombiner,
      SupplierFromVertex<I, V, E, M> messageSupplier,
      SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier,
      ConsumerWithVertex<I, V, E, M> messagesConsumer) {
    Preconditions.checkNotNull(messageCombiner);
    this.name = name;
    this.messageCombiner = messageCombiner;
    this.messageSupplier = messageSupplier;
    this.targetsSupplier = targetsSupplier;
    this.messagesConsumer = messagesConsumer;
  }

  /**
   * Stripe message sending computation across multiple stripes, in
   * each stripe only part of the vertices will receive messages.
   *
   * @param stripes Number of stripes
   * @param stripeSupplier Stripe supplier function, if IDs are Longs, you can
   *                       use StripingUtils::fastHashStripingPredicate
   * @return Resulting block
   */
  public Block stripeByReceiver(
      int stripes,
      Int2ObjFunction<Int2ObjFunction<Predicate<I>>> stripeSupplier) {
    return StripingUtils.generateStripedBlock(
        stripes,
        new Function<Predicate<I>, Block>() {
          @Override
          public Block apply(final Predicate<I> stripePredicate) {
            return FilteringPiece.createReceiveFiltering(
                new SupplierFromVertex<I, V, E, Boolean>() {
                  @Override
                  public Boolean get(Vertex<I, V, E> vertex) {
                    return stripePredicate.apply(vertex.getId());
                  }
                },
                new SendMessageWithCombinerPiece<>(
                  name,
                  messageCombiner,
                  messageSupplier,
                  new SupplierFromVertex<I, V, E, Iterator<I>>() {
                    @Override
                    public Iterator<I> get(Vertex<I, V, E> vertex) {
                      return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                          targetsSupplier.get(vertex), Spliterator.ORDERED),
                        false).filter(stripePredicate::apply).iterator();
                    }
                  },
                  messagesConsumer));
          }
        },
        stripeSupplier);
  }

  @Override
  public VertexSender<I, V, E> getVertexSender(
      final BlockWorkerSendApi<I, V, E, M> workerApi,
      Object executionStage) {
    return new InnerVertexSender() {
      @Override
      public void vertexSend(Vertex<I, V, E> vertex) {
        Iterator<I> targets = targetsSupplier.get(vertex);
        M message = messageSupplier.get(vertex);
        if (message != null && targets != null && targets.hasNext()) {
          workerApi.sendMessageToMultipleEdges(targets, message);
        }
      }
    };
  }

  @Override
  public VertexReceiver<I, V, E, M> getVertexReceiver(
      BlockWorkerReceiveApi<I> workerApi,
      Object executionStage) {
    return new InnerVertexReceiver() {
      @Override
      public void vertexReceive(Vertex<I, V, E> vertex, Iterable<M> messages) {
        Iterator<M> iter = messages.iterator();
        M combinedMessage = null;
        if (iter.hasNext()) {
          combinedMessage = iter.next();
          // When message combiner is used, there is never more then one message
          Preconditions.checkArgument(!iter.hasNext());
        }
        messagesConsumer.apply(vertex, combinedMessage);
      }
    };
  }

  @Override
  public MessageCombiner<? super I, M> getMessageCombiner(
      ImmutableClassesGiraphConfiguration conf) {
    return messageCombiner;
  }

  @Override
  public String toString() {
    return name;
  }
}
