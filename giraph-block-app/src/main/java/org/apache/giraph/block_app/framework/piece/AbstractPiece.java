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
package org.apache.giraph.block_app.framework.piece;

import java.util.Iterator;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexPostprocessor;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.function.Consumer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Iterators;

/**
 * Parent of all Pieces, contains comprehensive list of methods Piece
 * can support. Specific subclasses should be extended directly,
 * to simplify usage - most frequently for example Piece class.
 *
 * Single unit of execution, capturing:
 * - sending and then receiving messages from vertices
 * - sending data to be aggregated from workers to master
 * - sending values from master, via aggregators, to workers
 * - sending and receiving worker messages
 *
 *
 * Order of execution is:
 * - On master, once at the start of the application
 * -- registerAggregators (deprecated, use registerReducers instead)
 *
 * - After masterCompute of previous piece, on master:
 * -- registerReducers
 *
 * - Send logic on workers:
 * -- getVertexSender per each worker thread, and on object returned by it:
 * --- vertexSend on each vertex
 * --- postprocess on each worker thread
 * -- workerContextSend per worker
 *
 * - Logic on master:
 * -- masterCompute
 *
 * - Receive logic on workers:
 * -- workerContextReceive per worker
 * -- getVertexReceiver per each worker thread, and on object returned by it:
 * --- vertexReceive on each vertex
 * --- postprocess on each worker thread
 *
 * And before everything, during initialization, registerAggregators.
 *
 * Only masterCompute and registerReducers/registerAggregators should modify
 * the Piece, all of the worker methods should treat Piece as read-only.
 *
 * Each piece should be encapsulated unit of execution. Vertex value should be
 * used as a single implicit "communication" channel between different pieces,
 * all other dependencies should be explicitly defined and passed through
 * constructor, via interfaces (as explained below).
 * I.e. state of the vertex value is invariant that Pieces act upon.
 * Best is not to depend on explicit vertex value class, but on interface that
 * provides all needed functions, so that pieces can be freely combined,
 * as long as vertex value implements appropriate ones.
 * Similarly, use most abstract class you need - if Piece doesn't depend
 * on edge value, don't use NullWritable, but Writable. Or if it doesn't
 * depend on ExecutionStage, use Object for it.
 *
 * All other external dependencies should be explicitly passed through
 * constructor, through interfaces.
 *
 * All Pieces will be created within one context - on the master.
 * They are then going to be replicated across all workers, and across all
 * threads within each worker, and will see everything that happens in global
 * context (masterCompute) before them, including any state master has.
 * Through ObjectHolder/ObjectTransfer, you can pass data between Pieces in
 * global context, and from global context to worker functions of a Piece
 * that happens in the future.
 *
 * VertexReceiver of previous Piece and VertexSender of next Piece live in
 * the same context, and vertexReceive of the next Piece is executed
 * immediately after vertexSend of the previous piece, before vertexSend is
 * called on the next vertex.
 * This detail allows you to have external dependency on each other through
 * memory only mediator objects - like ObjectTransfer.
 *
 * All other logic going to live in different contexts,
 * specifically VertexSender and VertexReceiver of the same Piece,
 * or workerContextSend and VertexSender of the same Piece, and cannot interact
 * with each other outside of changing the state of the graph or using
 * global communication api.
 *
 * All methods on this class (or objects it returns) will be called serially,
 * so there is no need for any Thread synchronization.
 * Each Thread will have a complete deep copy of the Piece, to achieve that,
 * so all static fields must be written to be Thread safe!
 * (i.e. either immutable, or have synchronized/locked access to them)
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 * @param <WV> Worker value type
 * @param <WM> Worker message type
 * @param <S> Execution stage type
 */
@SuppressWarnings({ "rawtypes" })
public abstract class AbstractPiece<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable, WV,
    WM extends Writable, S> implements Block {

  // Overridable functions

  // registerReducers(CreateReducersApi reduceApi, S executionStage)

  /**
   * Add automatic handling of reducers to registerReducers.
   * Only for internal use.
   */
  public abstract void wrappedRegisterReducers(
      BlockMasterApi masterApi, S executionStage);

  // getVertexSender(BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage)

  /**
   * Add automatic handling of reducers to getVertexSender.
   *
   * Only for Framework internal use.
   */
  public abstract InnerVertexSender getWrappedVertexSender(
      final BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage);

  /**
   * Override to have worker context send computation.
   *
   * Called once per worker, after all vertices have been processed with
   * getVertexSender.
   */
  public void workerContextSend(
      BlockWorkerContextSendApi<I, WM> workerContextApi, S executionStage,
      WV workerValue) {
  }

  /**
   * Function that is called on master, after send phase, before receive phase.
   *
   * It can:
   * - read aggregators sent from worker
   * - do global processing
   * - send data to workers through aggregators
   */
  public void masterCompute(BlockMasterApi masterApi, S executionStage) {
  }

  /**
   * Override to have worker context receive computation.
   *
   * Called once per worker, before all vertices are going to be processed
   * with getVertexReceiver.
   */
  public void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi, S executionStage,
      WV workerValue, List<WM> workerMessages) {
  }

  /**
   * Override to do vertex receive processing.
   *
   * Creates handler that defines what should be executed on worker
   * for each vertex during receive phase.
   *
   * This logic executed last.
   * This function is called once on each worker on each thread, in parallel,
   * on their copy of Piece object to create functions handler.
   *
   * If returned object implements Postprocessor interface, then corresponding
   * postprocess() function is going to be called once, after all vertices
   * corresponding thread needed to process are done.
   */
  public VertexReceiver<I, V, E, M> getVertexReceiver(
      BlockWorkerReceiveApi<I> workerApi, S executionStage) {
    return null;
  }

  /**
   * Returns MessageClasses definition for messages being sent by this Piece.
   */
  public abstract MessageClasses<I, M> getMessageClasses(
      ImmutableClassesGiraphConfiguration conf);

  /**
   * Override to provide different next execution stage for
   * Pieces that come after it.
   *
   * Execution stage should be immutable, and this function should be
   * returning a new object, if it needs to return different value.
   *
   * It affects pieces that come after this piece,
   * and isn't applied to execution stage this piece sees.
   */
  public S nextExecutionStage(S executionStage) {
    return executionStage;
  }

  /**
   * Override to register any potential aggregators used by this piece.
   *
   * @deprecated Use registerReducers instead.
   */
  @Deprecated
  public void registerAggregators(BlockMasterApi masterApi)
      throws InstantiationException, IllegalAccessException {
  }

  // Inner classes

  /** Inner class to provide clean use without specifying types */
  public abstract class InnerVertexSender
      implements VertexSender<I, V, E>, VertexPostprocessor {
    @Override
    public void postprocess() { }
  }

  /** Inner class to provide clean use without specifying types */
  public abstract class InnerVertexReceiver
      implements VertexReceiver<I, V, E, M>, VertexPostprocessor {
    @Override
    public void postprocess() { }
  }

  // Internal implementation

  @Override
  public final Iterator<AbstractPiece> iterator() {
    return Iterators.<AbstractPiece>singletonIterator(this);
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    consumer.apply(this);
  }

  @Override
  public PieceCount getPieceCount() {
    return new PieceCount(1);
  }

  @Override
  public String toString() {
    String name = getClass().getSimpleName();
    if (name.isEmpty()) {
      name = getClass().getName();
    }
    return name;
  }


  // make hashCode and equals final, forcing them to be based on
  // reference identity.
  @Override
  public final int hashCode() {
    return super.hashCode();
  }

  @Override
  public final boolean equals(Object obj) {
    return super.equals(obj);
  }

}
