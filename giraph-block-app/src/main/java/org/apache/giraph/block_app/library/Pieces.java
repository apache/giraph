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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerAndBroadcastWrapperHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.array.BroadcastArrayHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.internal.SendMessagePiece;
import org.apache.giraph.block_app.library.internal.SendMessageWithCombinerPiece;
import org.apache.giraph.block_app.reducers.array.ArrayOfHandles;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.PairConsumer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Utility class for creating common Pieces and computations for processing
 * graphs.
 */
public class Pieces {
  private static final Logger LOG = Logger.getLogger(Pieces.class);

  private Pieces() { }

  /**
   * For each vertex execute given process function.
   * Computation is happening in send phase of the returned Piece.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Piece<I, V, E, NoMessage, Object> forAllVertices(
      final String pieceName, final Consumer<Vertex<I, V, E>> process) {
    return new Piece<I, V, E, NoMessage, Object>() {
      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<I, V, E> vertex) {
            process.apply(vertex);
          }
        };
      }

      @Override
      public String toString() {
        return pieceName;
      }
    };
  }

  /**
   * Execute given function on master.
   */
  public static
  Piece<WritableComparable, Writable,  Writable, NoMessage,
    Object> masterCompute(
      final String pieceName, final Consumer<BlockMasterApi> process) {
    return new Piece<WritableComparable, Writable,  Writable, NoMessage,
        Object>() {
      @Override
      public void masterCompute(
          BlockMasterApi masterApi, Object executionStage) {
        process.apply(masterApi);
      }
    };
  }

  /**
   * For each vertex execute given process function.
   * Computation is happening in the receive phase of the returned Piece.
   * This function should be used if you need returned Piece to interact with
   * subsequent Piece, as that requires passed function to be executed
   * during receive phase,
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Piece<I, V, E, NoMessage, Object> forAllVerticesOnReceive(
      final String pieceName, final Consumer<Vertex<I, V, E>> process) {
    return new Piece<I, V, E, NoMessage, Object>() {
      @Override
      public VertexReceiver<I, V, E, NoMessage> getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return new InnerVertexReceiver() {
          @Override
          public void vertexReceive(
              Vertex<I, V, E> vertex, Iterable<NoMessage> messages) {
            process.apply(vertex);
          }
        };
      }

      @Override
      public String toString() {
        return pieceName;
      }
    };
  }

  /**
   * Creates Piece which removes vertices for which supplier returns true.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Piece<I, V, E, NoMessage, Object> removeVertices(
      final String pieceName,
      final SupplierFromVertex<I, V, E, Boolean> shouldRemoveVertex) {
    return new Piece<I, V, E, NoMessage, Object>() {
      private ReducerHandle<LongWritable, LongWritable> countRemovedAgg;

      @Override
      public void registerReducers(
          CreateReducersApi reduceApi, Object executionStage) {
        countRemovedAgg = reduceApi.createLocalReducer(SumReduce.LONG);
      }

      @Override
      public VertexSender<I, V, E> getVertexSender(
          final BlockWorkerSendApi<I, V, E, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<I, V, E> vertex) {
            if (shouldRemoveVertex.get(vertex)) {
              workerApi.removeVertexRequest(vertex.getId());
              reduceLong(countRemovedAgg, 1);
            }
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        LOG.info("Removed " + countRemovedAgg.getReducedValue(master) +
            " vertices from the graph, during stage " + executionStage);
      }

      @Override
      public String toString() {
        return pieceName;
      }
    };
  }

  /**
   * Creates single reducer piece - given reduce class, supplier of values on
   * worker, reduces and passes the result to given consumer on master.
   *
   * @param <S> Single value type, objects passed on workers
   * @param <R> Reduced value type
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   */
  public static
  <S, R extends Writable, I extends WritableComparable, V extends Writable,
  E extends Writable>
  Piece<I, V, E, NoMessage, Object> reduce(
      String name,
      ReduceOperation<S, R> reduceOp,
      SupplierFromVertex<I, V, E, S> valueSupplier,
      final Consumer<R> reducedValueConsumer) {
    return reduceWithMaster(
        name, reduceOp, valueSupplier,
        new PairConsumer<R, BlockMasterApi>() {
          @Override
          public void apply(R input, BlockMasterApi master) {
            reducedValueConsumer.apply(input);
          }
        });
  }

  /**
   * Creates single reducer piece - given reduce class, supplier of values on
   * worker, reduces and passes the result to given consumer on master.
   *
   * @param <S> Single value type, objects passed on workers
   * @param <R> Reduced value type
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   */
  public static
  <S, R extends Writable, I extends WritableComparable, V extends Writable,
  E extends Writable>
  Piece<I, V, E, NoMessage, Object> reduceWithMaster(
      final String name,
      final ReduceOperation<S, R> reduceOp,
      final SupplierFromVertex<I, V, E, S> valueSupplier,
      final PairConsumer<R, BlockMasterApi> reducedValueConsumer) {
    return new Piece<I, V, E, NoMessage, Object>() {
      private ReducerHandle<S, R> handle;

      @Override
      public void registerReducers(
          CreateReducersApi reduceApi, Object executionStage) {
        handle = reduceApi.createLocalReducer(reduceOp);
      }

      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<I, V, E> vertex) {
            handle.reduce(valueSupplier.get(vertex));
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        reducedValueConsumer.apply(handle.getReducedValue(master), master);
      }

      @Override
      public String toString() {
        return name;
      }
    };
  }

  /**
   * Creates single reducer and broadcast piece - given reduce class, supplier
   * of values on worker, reduces and broadcasts the value, passing it to the
   * consumer on worker for each vertex.
   *
   * @param <S> Single value type, objects passed on workers
   * @param <R> Reduced value type
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   */
  public static
  <S, R extends Writable, I extends WritableComparable, V extends Writable,
  E extends Writable>
  Piece<I, V, E, NoMessage, Object> reduceAndBroadcast(
      final String name,
      final ReduceOperation<S, R> reduceOp,
      final SupplierFromVertex<I, V, E, S> valueSupplier,
      final ConsumerWithVertex<I, V, E, R> reducedValueConsumer) {
    return new Piece<I, V, E, NoMessage, Object>() {
      private final ReducerAndBroadcastWrapperHandle<S, R> handle =
          new ReducerAndBroadcastWrapperHandle<>();

      @Override
      public void registerReducers(
          CreateReducersApi reduceApi, Object executionStage) {
        handle.registeredReducer(reduceApi.createLocalReducer(reduceOp));
      }

      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<I, V, E> vertex) {
            handle.reduce(valueSupplier.get(vertex));
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        handle.broadcastValue(master);
      }

      @Override
      public VertexReceiver<I, V, E, NoMessage> getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        final R value = handle.getBroadcast(workerApi);
        return new InnerVertexReceiver() {
          @Override
          public void vertexReceive(
              Vertex<I, V, E> vertex, Iterable<NoMessage> messages) {
            reducedValueConsumer.apply(vertex, value);
          }
        };
      }

      @Override
      public String toString() {
        return name;
      }
    };
  }

  /**
   * Like reduceAndBroadcast, but uses array of handles for reducers and
   * broadcasts, to make it feasible and performant when values are large.
   * Each supplied value to reduce will be reduced in the handle defined by
   * handleHashSupplier%numHandles
   *
   * @param <S> Single value type, objects passed on workers
   * @param <R> Reduced value type
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   */
  public static
  <S, R extends Writable, I extends WritableComparable, V extends Writable,
      E extends Writable>
  Piece<I, V, E, NoMessage, Object> reduceAndBroadcastWithArrayOfHandles(
      final String name,
      final int numHandles,
      final Supplier<ReduceOperation<S, R>> reduceOp,
      final SupplierFromVertex<I, V, E, Long> handleHashSupplier,
      final SupplierFromVertex<I, V, E, S> valueSupplier,
      final ConsumerWithVertex<I, V, E, R> reducedValueConsumer) {
    return new Piece<I, V, E, NoMessage, Object>() {
      protected ArrayOfHandles.ArrayOfReducers<S, R> reducers;
      protected BroadcastArrayHandle<R> broadcasts;

      private int getHandleIndex(Vertex<I, V, E> vertex) {
        return (int) Math.abs(handleHashSupplier.get(vertex) % numHandles);
      }

      @Override
      public void registerReducers(
          final CreateReducersApi reduceApi, Object executionStage) {
        reducers = new ArrayOfHandles.ArrayOfReducers<>(
            numHandles,
            new Supplier<ReducerHandle<S, R>>() {
              @Override
              public ReducerHandle<S, R> get() {
                return reduceApi.createLocalReducer(reduceOp.get());
              }
            });
      }

      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<I, V, E> vertex) {
            reducers.get(getHandleIndex(vertex)).reduce(
                valueSupplier.get(vertex));
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        broadcasts = reducers.broadcastValue(master);
      }

      @Override
      public VertexReceiver<I, V, E, NoMessage> getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        final List<R> values = new ArrayList<>();
        for (int i = 0; i < numHandles; i++) {
          values.add(broadcasts.get(i).getBroadcast(workerApi));
        }
        return new InnerVertexReceiver() {
          @Override
          public void vertexReceive(
              Vertex<I, V, E> vertex, Iterable<NoMessage> messages) {
            reducedValueConsumer.apply(
                vertex, values.get(getHandleIndex(vertex)));
          }
        };
      }

      @Override
      public String toString() {
        return name;
      }
    };
  }

  /**
   * Creates Piece that for each vertex, sends message provided by
   * messageSupplier to all targets provided by targetsSupplier.
   * Received messages are then passed to and processed by provided
   * messagesConsumer.
   *
   * If messageSupplier or targetsSupplier returns null, current vertex
   * is not going to send any messages.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  M extends Writable>
  SendMessagePiece<I, V, E, M> sendMessage(
      String name,
      Class<M> messageClass,
      SupplierFromVertex<I, V, E, M> messageSupplier,
      SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier,
      ConsumerWithVertex<I, V, E, Iterable<M>> messagesConsumer) {
    return new SendMessagePiece<>(
        name, messageClass, messageSupplier, targetsSupplier, messagesConsumer);
  }

  /**
   * Creates Piece that for each vertex, sends message provided by
   * messageSupplier to all neighbors of current vertex.
   * Received messages are then passed to and processed by provided
   * messagesConsumer.
   *
   * If messageSupplier returns null, current vertex
   * is not going to send any messages.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  M extends Writable>
  SendMessagePiece<I, V, E, M> sendMessageToNeighbors(
      String name,
      Class<M> messageClass,
      SupplierFromVertex<I, V, E, M> messageSupplier,
      ConsumerWithVertex<I, V, E, Iterable<M>> messagesConsumer) {
    return sendMessage(
        name, messageClass, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier(),
        messagesConsumer);
  }

  /**
   * Creates Piece that for each vertex, sends message provided by
   * messageSupplier to all targets provided by targetsSupplier,
   * and uses given messageCombiner to combine messages together.
   * Received combined message is then passed to and processed by provided
   * messageConsumer. (null is passed to it, if vertex received no messages)
   *
   * If messageSupplier or targetsSupplier returns null, current vertex
   * is not going to send any messages.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  M extends Writable>
  SendMessageWithCombinerPiece<I, V, E, M> sendMessage(
      String name,
      MessageCombiner<? super I, M> messageCombiner,
      SupplierFromVertex<I, V, E, M> messageSupplier,
      SupplierFromVertex<I, V, E, Iterator<I>> targetsSupplier,
      ConsumerWithVertex<I, V, E, M> messagesConsumer) {
    return new SendMessageWithCombinerPiece<>(
        name, messageCombiner,
        messageSupplier, targetsSupplier, messagesConsumer);
  }

  /**
   * Creates Piece that for each vertex, sends message provided by
   * messageSupplier to all neighbors of current vertex,
   * and uses given messageCombiner to combine messages together.
   * Received combined message is then passed to and processed by provided
   * messageConsumer. (null is passed to it, if vertex received no messages)
   *
   * If messageSupplier returns null, current vertex
   * is not going to send any messages.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  M extends Writable>
  SendMessageWithCombinerPiece<I, V, E, M> sendMessageToNeighbors(
      String name,
      MessageCombiner<? super I, M> messageCombiner,
      SupplierFromVertex<I, V, E, M> messageSupplier,
      ConsumerWithVertex<I, V, E, M> messagesConsumer) {
    return sendMessage(
        name, messageCombiner, messageSupplier,
        VertexSuppliers.<I, V, E>vertexNeighborsSupplier(),
        messagesConsumer);
  }
}
