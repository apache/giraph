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
package org.apache.giraph.block_app.library.algo;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.TripleFunction;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * Class for running breadth-first search on the graph.
 * <p>
 * Graph is expected to be symmetric before calling any of the methods here.
 */
public class BreadthFirstSearch {
  private static final Logger LOG = Logger.getLogger(BreadthFirstSearch.class);
  private static final IntWritable NOT_REACHABLE_VERTEX_VALUE =
    new IntWritable(-1);

  private BreadthFirstSearch() {
  }

  /**
   * Default block for computing breadth-first search distances given functions
   * isVertexInSeedSet, getDistance and setDistance. This BFS computation block
   * computes only the shortest distance to seed vertices and does not compute
   * the closest seed.
   */
  public static <I extends WritableComparable, V extends Writable> Block bfs(
    SupplierFromVertex<I, V, Writable, Boolean> isVertexInSeedSet,
    SupplierFromVertex<I, V, Writable, IntWritable> getDistance,
    ConsumerWithVertex<I, V, Writable, IntWritable> setDistance) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<ByteWritable> vertexUpdatedDistance = new ObjectTransfer<>();

    IntWritable reusableInt = new IntWritable();
    ByteWritable emptyByteWritable = new ByteWritable();
    SupplierFromVertex<I, V, Writable, ByteWritable>
      initializeVertex = (vertex) -> {
      if (isVertexInSeedSet.get(vertex)) {
        reusableInt.set(0);
        setDistance.apply(vertex, reusableInt);
        return emptyByteWritable;
      } else {
        reusableInt.set(-1);
        setDistance.apply(vertex, reusableInt);
        return null;
      }
    };

    IntWritable notReachableVertex = new IntWritable(-1);
    TripleFunction<Vertex<I, V, Writable>, IntWritable, Iterator<ByteWritable>,
      ByteWritable> traverseVertex = (vertex, distance, messageIter) -> {
      if (getDistance.get(vertex).compareTo(notReachableVertex) == 0 ||
        getDistance.get(vertex).compareTo(distance) > 0) {
        setDistance.apply(vertex, distance);
        return emptyByteWritable;
      } else {
        return null;
      }
    };

    Class<ByteWritable> messageClass = ByteWritable.class;
    return new SequenceBlock(
      createInitializePiece(vertexUpdatedDistance, initializeVertex),
      RepeatUntilBlock.unlimited(
        createPropagateConnectedComponentsPiece(messageClass,
          vertexUpdatedDistance, vertexUpdatedDistance, converged,
          traverseVertex, VertexSuppliers.vertexEdgesSupplier()), converged));
  }

  /**
   * Default block for computing breadth-first search distances given functions
   * initializeVertex and traverseVertex. This BFS computation block computes
   * both the shortest distance to seed vertices as well as the closest seed
   * for all vertices.
   */
  public static
  <I extends WritableComparable, V extends Writable, M extends Writable>
  Block bfs(Class<M> messageType,
    SupplierFromVertex<I, V, Writable, M> initializeVertex,
    TripleFunction<Vertex<I, V, Writable>, IntWritable, Iterator<M>, M>
      traverseVertex) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<M> vertexUpdatedDistance = new ObjectTransfer<>();

    return new SequenceBlock(
      createInitializePiece(vertexUpdatedDistance, initializeVertex),
      RepeatUntilBlock.unlimited(
        createPropagateConnectedComponentsPiece(messageType,
          vertexUpdatedDistance, vertexUpdatedDistance, converged,
          traverseVertex, VertexSuppliers.vertexEdgesSupplier()), converged));
  }

  /**
   * Initialize vertex values for BFS computation.
   */
  private static
  <I extends WritableComparable, V extends Writable, M extends Writable>
  Piece<I, V, Writable, NoMessage, Object> createInitializePiece(
    Consumer<M> vertexUpdatedDistance,
    SupplierFromVertex<I, V, Writable, M> initializeVertex) {
    return Pieces.forAllVerticesOnReceive("InitializeBFS", (vertex) -> {
      vertexUpdatedDistance.apply(initializeVertex.get(vertex));
    });
  }

  /**
   * Propagate shortest distance to to neighbor pieces using connected
   * components piece.
   */
  private static
  <I extends WritableComparable, V extends Writable, M extends Writable>
  Block createPropagateConnectedComponentsPiece(
    Class<M> messageClass, Supplier<M> vertexToPropagate,
    Consumer<M> vertexUpdatedDistance, Consumer<Boolean> converged,
    TripleFunction<Vertex<I, V, Writable>, IntWritable, Iterator<M>, M>
      traverseVertex,
    SupplierFromVertex<I, V, Writable, ? extends Iterable<?
      extends Edge<I, ?>>> edgeSupplier) {
    return new Piece<I, V, Writable, M, Object>() {
      private IntWritable globalDistance = new IntWritable(0);
      private ReducerHandle<IntWritable, IntWritable> propagatedAggregator;

      @Override public void registerReducers(CreateReducersApi reduceApi,
        Object executionStage) {
        propagatedAggregator = reduceApi.createLocalReducer(SumReduce.INT);
      }

      @Override public VertexSender<I, V, Writable> getVertexSender(
        BlockWorkerSendApi<I, V, Writable, M> workerApi,
        Object executionStage) {
        return (vertex) -> {
          M messageToSend = vertexToPropagate.get();
          if (messageToSend != null) {
            workerApi.sendMessageToMultipleEdges(Iterators
              .transform(edgeSupplier.get(vertex).iterator(),
                Edge::getTargetVertexId), messageToSend);
            reduceInt(propagatedAggregator, 1);
          }
        };
      }

      @Override public void masterCompute(BlockMasterApi master,
        Object executionStage) {
        converged
          .apply(propagatedAggregator.getReducedValue(master).get() == 0);
        LOG.info("BFS: " + propagatedAggregator.getReducedValue(master).get() +
          " many vertices sent in this iteration");
        globalDistance.set(globalDistance.get() + 1);
      }

      @Override public VertexReceiver<I, V, Writable, M> getVertexReceiver(
        BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return (vertex, messages) -> {
          if (messages.iterator().hasNext()) {
            vertexUpdatedDistance.apply(traverseVertex
              .apply(vertex, globalDistance, messages.iterator()));
          }
        };
      }

      @Override public Class<M> getMessageClass() {
        return messageClass;
      }

      @Override public String toString() {
        return "BreadthFirstSearchPiece";
      }
    };
  }
}
