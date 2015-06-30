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
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * Class for running breadth-first search on the graph.
 *
 * Graph is expected to be symmetric before calling any of the methods here.
 */
public class BreadthFirstSearch {
  private static final Logger LOG = Logger.getLogger(BreadthFirstSearch.class);
  private static final IntWritable NOT_REACHABLE_VERTEX_VALUE =
      new IntWritable(-1);

  private BreadthFirstSearch() {
  }

  /**
   * Default block, which calculates connected components using the vertex's
   * default edges.
   */
  public static <I extends WritableComparable, V extends Writable>
  Block bfs(
    SupplierFromVertex<I, V, Writable, Boolean> isVertexInSeedSet,
    SupplierFromVertex<I, V, Writable, IntWritable> getDistance,
    ConsumerWithVertex<I, V, Writable, IntWritable> setDistance
  ) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<Boolean> vertexUpdatedDistance = new ObjectTransfer<>();

    return new SequenceBlock(
      createInitializePiece(
        vertexUpdatedDistance,
        isVertexInSeedSet,
        getDistance,
        setDistance,
        VertexSuppliers.vertexEdgesSupplier()
      ),
      RepeatUntilBlock.unlimited(
        createPropagateConnectedComponentsPiece(
          vertexUpdatedDistance,
          vertexUpdatedDistance,
          converged,
          getDistance,
          setDistance,
          VertexSuppliers.vertexEdgesSupplier()
        ),
        converged
      )
    );
  }

  /**
   * Initialize vertex values for connected components calculation
   */
  private static <I extends WritableComparable, V extends Writable>
  Piece<I, V, Writable, NoMessage, Object> createInitializePiece(
    Consumer<Boolean> vertexUpdatedDistance,
    SupplierFromVertex<I, V, Writable, Boolean> isVertexInSeedSet,
    SupplierFromVertex<I, V, Writable, IntWritable> getDistance,
    ConsumerWithVertex<I, V, Writable, IntWritable> setDistance,
    SupplierFromVertex<I, V, Writable, ? extends Iterable<? extends Edge<I, ?>>>
      edgeSupplier
  ) {
    IntWritable zero = new IntWritable(0);
    return Pieces.forAllVerticesOnReceive(
      "InitializeBFS",
      (vertex) -> {
        if (isVertexInSeedSet.get(vertex)) {
          setDistance.apply(vertex, zero);
          vertexUpdatedDistance.apply(true);
        } else {
          setDistance.apply(vertex, NOT_REACHABLE_VERTEX_VALUE);
          vertexUpdatedDistance.apply(false);
        }
      }
    );
  }

  /**
   * Propagate connected components to neighbor pieces
   */
  private static <I extends WritableComparable, V extends Writable>
  Block createPropagateConnectedComponentsPiece(
      Supplier<Boolean> vertexToPropagate,
      Consumer<Boolean> vertexUpdatedDistance,
      Consumer<Boolean> converged,
      SupplierFromVertex<I, V, Writable, IntWritable> getDistance,
      ConsumerWithVertex<I, V, Writable, IntWritable> setDistance,
      SupplierFromVertex<I, V, Writable, ? extends Iterable<?
        extends Edge<I, ?>>> edgeSupplier) {
    return new Piece<I, V, Writable, IntWritable, Object>() {
      private ReducerHandle<IntWritable, IntWritable> propagatedAggregator;

      @Override
      public void registerReducers(
          CreateReducersApi reduceApi, Object executionStage) {
        propagatedAggregator = reduceApi.createLocalReducer(SumReduce.INT);
      }

      @Override
      public VertexSender<I, V, Writable> getVertexSender(
        BlockWorkerSendApi<I, V, Writable, IntWritable> workerApi,
        Object executionStage
      ) {
        return (vertex) -> {
          if (vertexToPropagate.get()) {
            workerApi.sendMessageToMultipleEdges(
              Iterators.transform(
                edgeSupplier.get(vertex).iterator(),
                Edge::getTargetVertexId
              ),
              getDistance.get(vertex)
            );
            reduceInt(propagatedAggregator, 1);
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        converged.apply(
            propagatedAggregator.getReducedValue(master).get() == 0);
        LOG.info("BFS: " + propagatedAggregator.getReducedValue(master).get() +
                 " many vertices sent in this iteration");
      }

      @Override
      public VertexReceiver<I, V, Writable, IntWritable> getVertexReceiver(
        BlockWorkerReceiveApi<I> workerApi,
        Object executionStage
      ) {
        IntWritable next = new IntWritable();
        return (vertex, messages) -> {
          vertexUpdatedDistance.apply(false);
          for (IntWritable receivedValue : messages) {
            IntWritable currentValue = getDistance.get(vertex);
            next.set(receivedValue.get() + 1);
            if (currentValue.compareTo(NOT_REACHABLE_VERTEX_VALUE) == 0 ||
                currentValue.compareTo(next) > 0) {
              setDistance.apply(vertex, next);
              vertexUpdatedDistance.apply(true);
            }
          }
        };
      }

      @Override
      public Class<IntWritable> getMessageClass() {
        return IntWritable.class;
      }

      @Override
      public String toString() {
        return "PropagateConnectedComponentsPiece";
      }
    };
  }
}
