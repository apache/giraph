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
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;

/**
 * Class for computing maximal independent sets of a graph.
 *
 * Graph is expected to be symmetric before calling methods here.
 */
public class DistributedIndependentSet {
  private static final Logger LOG =
      Logger.getLogger(DistributedIndependentSet.class);
  private static final IntWritable UNKNOWN = new IntWritable(-1);
  private static final IntWritable NOT_IN_SET = new IntWritable(-2);
  private static final IntWritable IN_SET = new IntWritable(-3);

  private DistributedIndependentSet() {
  }

  /**
   * Default block which decomposes the input graph into independent sets of
   * vertices. An independent set of vertices is defined as a set of vertices
   * that do not have edge to each other, i.e. the sub-graph induced by an
   * independent set of vertices is an empty graph.
   *
   * The algorithm finds independent sets as follows:
   *   1) Find the maximal independent set of vertices amongst unassigned
   *      vertices.
   *   2) Assign found vertices to a new independent set.
   *   3) If all vertices of the graph are assigned, go to step 1. Otherwise
   *      terminate.
   *
   * @param numberClass Independent set id type
   * @param chooseNumber Process that returns a deterministic id of type
   *                     <code>numberClass</code> for each vertex
   * @param getIndependentSet Getter for independent set id of each vertex
   * @param setIndependentSet Setter for independent set id of each vertex
   */
  public static
  <I extends WritableComparable, V extends Writable,
  N extends WritableComparable>
  Block independentSets(
      Class<N> numberClass,
      SupplierFromVertex<I, V, Writable, N> chooseNumber,
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet
  ) {
    ObjectTransfer<Boolean> done = new ObjectTransfer<>();
    IntRef iteration = new IntRef(-1);

    return new SequenceBlock(
        Pieces.<I, V, Writable>forAllVerticesOnReceive(
            "InitializeIndependentSet",
            (vertex) -> setIndependentSet.apply(vertex, UNKNOWN)),
        RepeatUntilBlock.unlimited(
            // find maximal independent sets amongst remaining un-assigned
            // vertices, one after another, until all vertices of the graph
            // are assigned to independent sets.
            findMaximalIndependentSet(
                numberClass,
                chooseNumber,
                getIndependentSet,
                setIndependentSet,
                iteration, done
            ),
            done
        )
    );
  }

  /**
   * Independent set calculation with vertex ids as messages to choose the
   * vertices in a set.
   */
  public static <I extends WritableComparable, V extends Writable>
  Block independentSets(
      Class<I> vertexIdClass,
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet
  ) {
    return independentSets(
        vertexIdClass,
        VertexSuppliers.vertexIdSupplier(),
        getIndependentSet,
        setIndependentSet
    );
  }

  /**
     * Finds a maximal independent set, amongst un-assigned vertices of the
     * graph. The algorithm is as follows:
     *   1) Normalize the state of all un-assigned vertices of the graph to
     *      UNKNOWN.
     *   2) Each UNKNOWN vertex sends a number (output of <code>chooseNumber
     *      </code>) to all its neighbors.
     *   3) Each UNKNOWN vertex finds the maximum value of all incoming
     *      messages. If the max value is less than the chosen number for the
     *      vertex (output of <code>chooseNumber</code> in step 2), the vertex
     *      assigns itself to the independent set (changes its state to IN_SET),
     *      and sends 'ack' messages to all its neighbors.
     *   4) Each UNKNOWN vertex that receives an 'ack' message, changes its
     *      state to NOT_IN_SET.
     *   5) If there are any UNKNOWN vertex, go to step 2. Otherwise, we found
     *      the maximal independent set, and terminate.
     */
  private static
  <I extends WritableComparable, V extends Writable,
  N extends WritableComparable>
  Block findMaximalIndependentSet(
      Class<N> numberClass,
      SupplierFromVertex<I, V, Writable, N> chooseNumber,
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet,
      IntRef iteration, ObjectTransfer<Boolean> done
  ) {
    ObjectTransfer<Boolean> foundMIS = new ObjectTransfer<>();
    return new SequenceBlock(
        createInitializePiece(getIndependentSet, setIndependentSet, iteration),
        RepeatUntilBlock.unlimited(
            new SequenceBlock(
                // Announce the number to all the neighbors.
                createAnnounceBlock(
                    numberClass,
                    chooseNumber,
                    getIndependentSet,
                    setIndependentSet
                ),
                // Select the vertices in the independent set, and refine the
                // state of neighboring vertices so not to consider them for
                // this independent set.
                createSelectAndRefinePiece(
                    getIndependentSet,
                    setIndependentSet,
                    iteration,
                    foundMIS,
                    done
                )
            ),
            foundMIS
        )
    );
  }

  /**
   * Piece to initialize vertex values so they are either assigned to a
   * previously found independent set, or UNKNOWN to be considered for the
   * discovery of the current independent set.
   */
  private static <I extends WritableComparable, V extends Writable>
  Block createInitializePiece(
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet,
      IntRef iteration) {
    return new Piece<I, V, Writable, NoMessage, Object>() {
      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        iteration.value++;
        LOG.info("Start finding independent set with ID " + iteration.value);
      }

      @Override
      public VertexReceiver<I, V, Writable, NoMessage> getVertexReceiver(
          final BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return (vertex, messages) -> {
          if (getIndependentSet.get(vertex).equals(NOT_IN_SET)) {
            setIndependentSet.apply(vertex, UNKNOWN);
          }
        };
      }

      @Override
      public String toString() {
        return "InitializePiece";
      }
    };
  }

  /**
   * A Block that assigns some of the UNKNOWN vertices to the new independent
   * set based on numbers each vertex broadcast to all its neighbors.
   */
  private static
  <I extends WritableComparable, V extends Writable,
  N extends WritableComparable>
  Block createAnnounceBlock(
      Class<N> numberClass,
      SupplierFromVertex<I, V, Writable, N> chooseNumber,
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet) {
    return Pieces.<I, V, Writable, N>sendMessageToNeighbors(
        "AnnounceNumber",
        numberClass,
        (vertex) -> {
          if (getIndependentSet.get(vertex).equals(UNKNOWN)) {
            return chooseNumber.get(vertex);
          } else {
            return null;
          }
        },
        (vertex, messages) -> {
          if (getIndependentSet.get(vertex).equals(UNKNOWN)) {
            N curVal = chooseNumber.get(vertex);
            boolean myValIsMax = true;
            for (N receivedValue : messages) {
              if (receivedValue.compareTo(curVal) > 0) {
                myValIsMax = false;
                break;
              }
            }
            if (myValIsMax) {
              setIndependentSet.apply(vertex, IN_SET);
            }
          }
        }
    );
  }

  /**
   * Piece to confirm selection of some vertices for the independent set. Also,
   * changes the state of neighboring vertices of newly assigned vertices to
   * NOT_IN_SET, so not to consider them for the discovery of the current
   * independent set.
   *
   * @param foundMIS Specifies the end of discovery for current independent set.
   * @param done Specifies the end of whole computation of decomposing to
   *             independent sets.
   */
  private static <I extends WritableComparable, V extends Writable>
  Block createSelectAndRefinePiece(
      SupplierFromVertex<I, V, Writable, IntWritable> getIndependentSet,
      ConsumerWithVertex<I, V, Writable, IntWritable> setIndependentSet,
      IntRef iteration,
      Consumer<Boolean> foundMIS,
      Consumer<Boolean> done) {
    return new Piece<I, V, Writable, BooleanWritable, Object>() {
      private ReducerHandle<IntWritable, IntWritable> numVerticesUnknown;
      private ReducerHandle<IntWritable, IntWritable> numVerticesNotAssigned;

      @Override
      public void registerReducers(
          CreateReducersApi reduceApi, Object executionStage) {
        numVerticesUnknown = reduceApi.createLocalReducer(SumReduce.INT);
        numVerticesNotAssigned = reduceApi.createLocalReducer(SumReduce.INT);
      }

      @Override
      public VertexSender<I, V, Writable> getVertexSender(
          final BlockWorkerSendApi<I, V, Writable, BooleanWritable> workerApi,
          Object executionStage) {
        BooleanWritable ack = new BooleanWritable(true);
        IntWritable one = new IntWritable(1);
        return (vertex) -> {
          IntWritable vertexState = getIndependentSet.get(vertex);
          if (vertexState.equals(IN_SET)) {
            setIndependentSet.apply(vertex, new IntWritable(iteration.value));
            workerApi.sendMessageToAllEdges(vertex, ack);
          } else if (vertexState.equals(UNKNOWN)) {
            numVerticesUnknown.reduce(one);
            numVerticesNotAssigned.reduce(one);
          } else if (vertexState.equals(NOT_IN_SET)) {
            numVerticesNotAssigned.reduce(one);
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi master, Object executionStage) {
        done.apply(numVerticesNotAssigned.getReducedValue(master).get() == 0);
        foundMIS.apply(numVerticesUnknown.getReducedValue(master).get() == 0);
      }

      @Override
      public VertexReceiver<I, V, Writable, BooleanWritable> getVertexReceiver(
          final BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return (vertex, messages) -> {
          if (getIndependentSet.get(vertex).equals(UNKNOWN) &&
              Iterables.size(messages) > 0) {
            setIndependentSet.apply(vertex, NOT_IN_SET);
          }
        };
      }

      @Override
      public Class<BooleanWritable> getMessageClass() {
        return BooleanWritable.class;
      }

      @Override
      public String toString() {
        return "SelectAndRefinePiece";
      }
    };
  }
}
