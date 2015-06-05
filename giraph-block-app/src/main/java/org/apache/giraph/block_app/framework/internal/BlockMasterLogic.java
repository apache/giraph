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
package org.apache.giraph.block_app.framework.internal;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.BlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockOutputHandleAccessor;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.log4j.Logger;
import org.python.google.common.base.Preconditions;

/**
 * Block execution logic on master, iterating over Pieces of the
 * application Block, executing master logic, and providing what needs to be
 * executed on the workers.
 *
 * @param <S> Execution stage type
 */
@SuppressWarnings("rawtypes")
public class BlockMasterLogic<S> {
  private static final Logger LOG = Logger.getLogger(BlockMasterLogic.class);

  private Iterator<AbstractPiece> pieceIterator;
  private PairedPieceAndStage<S> previousPiece;
  private transient BlockMasterApi masterApi;
  private long lastTimestamp = -1;
  private BlockWorkerPieces previousWorkerPieces;
  private boolean computationDone;

  public void initialize(
      GiraphConfiguration conf, final BlockMasterApi masterApi)
    throws InstantiationException, IllegalAccessException {
    this.masterApi = masterApi;
    this.computationDone = false;

    BlockFactory<S> factory = BlockUtils.createBlockFactory(conf);
    Block executionBlock = factory.createBlock(conf);
    LOG.info("Executing application - " + executionBlock);

    // We register all possible aggregators at the beginning
    executionBlock.forAllPossiblePieces(new Consumer<AbstractPiece>() {
      private final HashSet<AbstractPiece> registeredPieces = new HashSet<>();
      @SuppressWarnings("deprecation")
      @Override
      public void apply(AbstractPiece piece) {
        // no need to regiser the same piece twice.
        if (registeredPieces.add(piece)) {
          try {
            piece.registerAggregators(masterApi);
          } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
    });

    pieceIterator = executionBlock.iterator();
    // Invariant is that ReceiveWorkerPiece of previousPiece has already been
    // executed and that previousPiece.nextExecutionStage() should be used for
    // iterating. So passing piece as null, and initial state as current state,
    // so that nothing get's executed in first half, and calculateNextState
    // returns initial state.
    previousPiece = new PairedPieceAndStage<>(
        null, factory.createExecutionStage(conf));
  }

  /**
   * Initialize object after deserializing it.
   * BlockMasterApi is not serializable, so it is transient, and set via this
   * method afterwards.
   */
  public void initializeAfterRead(BlockMasterApi masterApi) {
    this.masterApi = masterApi;
  }

  /**
   * Executes operations on master (master compute and registering reducers),
   * and calculates next pieces to be exectued on workers.
   *
   * @param superstep Current superstep
   * @return Next BlockWorkerPieces to be executed on workers, or null
   *         if computation should be halted.
   */
  public BlockWorkerPieces<S> computeNext(long superstep) {
    long beforeMaster = System.currentTimeMillis();
    if (lastTimestamp != -1) {
      BlockCounters.setWorkerTimeCounter(
          previousWorkerPieces, superstep - 1,
          beforeMaster - lastTimestamp, masterApi);
    }

    if (previousPiece == null) {
      postApplication();
      return null;
    } else {
      LOG.info(
          "Master executing " + previousPiece + ", in superstep " + superstep);
      previousPiece.masterCompute(masterApi);
      ((BlockOutputHandleAccessor) masterApi).getBlockOutputHandle().
          returnAllWriters();
      long afterMaster = System.currentTimeMillis();

      if (previousPiece.getPiece() != null) {
        BlockCounters.setMasterTimeCounter(
            previousPiece, superstep, afterMaster - beforeMaster, masterApi);
      }

      PairedPieceAndStage<S> nextPiece;
      if (pieceIterator.hasNext()) {
        nextPiece = new PairedPieceAndStage<S>(
            pieceIterator.next(), previousPiece.nextExecutionStage());
        nextPiece.registerReducers(masterApi);
      } else {
        nextPiece = null;
      }
      BlockCounters.setStageCounters(
          "Master finished stage: ", previousPiece.getExecutionStage(),
          masterApi);
      LOG.info(
          "Master passing next " + nextPiece + ", in superstep " + superstep);

      // if there is nothing more to compute, no need for additional superstep
      // this can only happen if application uses no pieces.
      BlockWorkerPieces<S> result;
      if (previousPiece.getPiece() == null && nextPiece == null) {
        postApplication();
        result = null;
      } else {
        result = new BlockWorkerPieces<>(previousPiece, nextPiece);
        LOG.info("Master in " + superstep + " superstep passing " +
            result + " to be executed");
      }

      previousPiece = nextPiece;
      lastTimestamp = afterMaster;
      previousWorkerPieces = result;
      return result;
    }
  }

  /**
   * Clean up any master state, after application has finished.
   */
  private void postApplication() {
    ((BlockOutputHandleAccessor) masterApi).getBlockOutputHandle().
        closeAllWriters();
    Preconditions.checkState(!computationDone);
    computationDone = true;
  }

}
