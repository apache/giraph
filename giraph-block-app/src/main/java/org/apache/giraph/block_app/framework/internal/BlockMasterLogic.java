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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.giraph.block_app.framework.BlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockOutputHandleAccessor;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.BlockWithApiHandle;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.writable.tuple.IntLongWritable;
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
  private BlockApiHandle blockApiHandle;

  /** Tracks elapsed time on master for each distinct Piece */
  private final TimeStatsPerEvent masterPerPieceTimeStats =
      new TimeStatsPerEvent("master");
  /** Tracks elapsed time on workers for each pair of recieve/send pieces. */
  private final TimeStatsPerEvent workerPerPieceTimeStats =
      new TimeStatsPerEvent("worker");

  /**
   * Initialize master logic to execute BlockFactory defined in
   * the configuration.
   */
  public void initialize(
      GiraphConfiguration conf, final BlockMasterApi masterApi) {
    BlockFactory<S> factory = BlockUtils.createBlockFactory(conf);
    initialize(factory.createBlock(conf), factory.createExecutionStage(conf),
        masterApi);
  }

  /**
   * Initialize Master Logic to execute given block, starting
   * with given executionStage.
   */
  public void initialize(
      Block executionBlock, S executionStage, final BlockMasterApi masterApi) {
    this.masterApi = masterApi;
    this.computationDone = false;

    LOG.info("Executing application - " + executionBlock);
    if (executionBlock instanceof BlockWithApiHandle) {
      blockApiHandle =
        ((BlockWithApiHandle) executionBlock).getBlockApiHandle();
    }
    if (blockApiHandle == null) {
      blockApiHandle = new BlockApiHandle();
    }
    blockApiHandle.setMasterApi(masterApi);

    // We register all possible aggregators at the beginning
    executionBlock.forAllPossiblePieces(new Consumer<AbstractPiece>() {
      private final HashSet<AbstractPiece> registeredPieces = new HashSet<>();
      @SuppressWarnings("deprecation")
      @Override
      public void apply(AbstractPiece piece) {
        // no need to register the same piece twice.
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
    previousPiece = new PairedPieceAndStage<>(null, executionStage);
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
          beforeMaster - lastTimestamp, masterApi, workerPerPieceTimeStats);
    }

    if (previousPiece == null) {
      postApplication();
      return null;
    } else {
      boolean logExecutionStatus =
          BlockUtils.LOG_EXECUTION_STATUS.get(masterApi.getConf());
      if (logExecutionStatus) {
        LOG.info("Master executing " + previousPiece +
            ", in superstep " + superstep);
      }
      previousPiece.masterCompute(masterApi);
      ((BlockOutputHandleAccessor) masterApi).getBlockOutputHandle().
          returnAllWriters();
      long afterMaster = System.currentTimeMillis();

      if (previousPiece.getPiece() != null) {
        BlockCounters.setMasterTimeCounter(
            previousPiece, superstep, afterMaster - beforeMaster, masterApi,
            masterPerPieceTimeStats);
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
      if (logExecutionStatus) {
        LOG.info(
            "Master passing next " + nextPiece + ", in superstep " + superstep);
      }

      // if there is nothing more to compute, no need for additional superstep
      // this can only happen if application uses no pieces.
      BlockWorkerPieces<S> result;
      if (previousPiece.getPiece() == null && nextPiece == null) {
        postApplication();
        result = null;
      } else {
        result = new BlockWorkerPieces<>(
          previousPiece, nextPiece, blockApiHandle);
        if (logExecutionStatus) {
          LOG.info("Master in " + superstep + " superstep passing " +
              result + " to be executed");
        }
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
    IntLongWritable masterTimes = masterPerPieceTimeStats.logTimeSums();
    IntLongWritable workerTimes = workerPerPieceTimeStats.logTimeSums();
    LOG.info("Time split:\n" +
        TimeStatsPerEvent.header() +
        TimeStatsPerEvent.line(
            masterTimes.getLeft().get(),
            100.0 * masterTimes.getRight().get() /
              (masterTimes.getRight().get() + workerTimes.getRight().get()),
            masterTimes.getRight().get(),
            "master") +
        TimeStatsPerEvent.line(
            workerTimes.getLeft().get(),
            100.0 * workerTimes.getRight().get() /
              (masterTimes.getRight().get() + workerTimes.getRight().get()),
            workerTimes.getRight().get(),
            "worker"));
  }

  /**
   * Class tracking invocation count and elapsed time for a set of events,
   * each event being having a String name.
   */
  public static class TimeStatsPerEvent {
    private final String groupName;
    private final Map<String, IntLongWritable> keyToCountAndTime =
        new TreeMap<>();

    public TimeStatsPerEvent(String groupName) {
      this.groupName = groupName;
    }

    public void inc(String name, long millis) {
      IntLongWritable val = keyToCountAndTime.get(name);
      if (val == null) {
        val = new IntLongWritable();
        keyToCountAndTime.put(name, val);
      }
      val.getLeft().set(val.getLeft().get() + 1);
      val.getRight().set(val.getRight().get() + millis);
    }

    public IntLongWritable logTimeSums() {
      StringBuilder sb = new StringBuilder("Time sums " + groupName + ":\n");
      sb.append(header());
      long total = 0;
      int count = 0;
      for (Entry<String, IntLongWritable> entry :
            keyToCountAndTime.entrySet()) {
        total += entry.getValue().getRight().get();
        count += entry.getValue().getLeft().get();
      }

      for (Entry<String, IntLongWritable> entry :
            keyToCountAndTime.entrySet()) {
        sb.append(line(
            entry.getValue().getLeft().get(),
            (100.0 * entry.getValue().getRight().get()) / total,
            entry.getValue().getRight().get(),
            entry.getKey()));
      }
      LOG.info(sb);
      return new IntLongWritable(count, total);
    }

    public static String header() {
      return String.format(
          "%10s%10s%11s   %s%n", "count", "time %", "time", "name");
    }

    public static String line(
        int count, double percTime, long time, String name) {
      return String.format("%10d%9.2f%%%11s   %s%n",
          count,
          percTime,
          DurationFormatUtils.formatDuration(time, "HH:mm:ss"),
          name);
    }
  }
}
