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

package org.apache.giraph.ooc;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.command.StoreDataBufferIOCommand;
import org.apache.giraph.ooc.command.StoreIncomingMessageIOCommand;
import org.apache.giraph.ooc.command.StorePartitionIOCommand;
import org.apache.giraph.ooc.command.WaitIOCommand;
import org.apache.giraph.ooc.policy.OutOfCoreOracle;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of IO thread scheduler for out-of-core mechanism
 */
public class OutOfCoreIOScheduler {
  /**
   * If an IO thread does not have any command to do, it waits for certain a
   * period and check back again to see if there exist any command to perform.
   * This constant determines this wait period in milliseconds.
   */
  public static final IntConfOption OOC_WAIT_INTERVAL =
      new IntConfOption("giraph.oocWaitInterval", 1000,
          "Duration (in milliseconds) which IO threads in out-of-core " +
              "mechanism would wait until a command becomes available");
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(OutOfCoreIOScheduler.class);
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** How much an IO thread should wait if there is no IO command */
  private final int waitInterval;
  /**
   * Queue of IO commands for loading partitions to memory. Load commands are
   * urgent and should be done once loading data is a viable IO command.
   */
  private final List<Queue<IOCommand>> threadLoadCommandQueue;
  /** Whether IO threads should terminate */
  private volatile boolean shouldTerminate;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param oocEngine out-of-core engine
   * @param numDisks number of disks (IO threads)
   */
  OutOfCoreIOScheduler(final ImmutableClassesGiraphConfiguration conf,
                       OutOfCoreEngine oocEngine, int numDisks) {
    this.oocEngine = oocEngine;
    this.waitInterval = OOC_WAIT_INTERVAL.get(conf);
    threadLoadCommandQueue = new ArrayList<>(numDisks);
    for (int i = 0; i < numDisks; ++i) {
      threadLoadCommandQueue.add(
          new ConcurrentLinkedQueue<IOCommand>());
    }
    shouldTerminate = false;
  }

  /**
   * Generate and return the next appropriate IO command for a given thread
   *
   * @param threadId id of the thread ready to execute the next IO command
   * @return next IO command to be executed by the given thread
   */
  public IOCommand getNextIOCommand(int threadId) {
    if (shouldTerminate) {
      return null;
    }
    IOCommand command = null;
    do {
      if (command != null && LOG.isInfoEnabled()) {
        LOG.info("getNextIOCommand: command " + command + " was proposed to " +
            "the oracle, but got denied. Generating another command!");
      }
      OutOfCoreOracle.IOAction[] actions =
          oocEngine.getOracle().getNextIOActions();
      if (LOG.isDebugEnabled()) {
        LOG.debug("getNextIOCommand: actions are " + Arrays.toString(actions));
      }
      // Check whether there are any urgent outstanding load requests
      if (!threadLoadCommandQueue.get(threadId).isEmpty()) {
        // Check whether loading a partition is a viable (allowed) action to do
        boolean canLoad = false;
        for (OutOfCoreOracle.IOAction action : actions) {
          if (action == OutOfCoreOracle.IOAction.LOAD_PARTITION ||
              action == OutOfCoreOracle.IOAction.LOAD_UNPROCESSED_PARTITION ||
              action == OutOfCoreOracle.IOAction.LOAD_TO_SWAP_PARTITION ||
              action == OutOfCoreOracle.IOAction.URGENT_LOAD_PARTITION) {
            canLoad = true;
            break;
          }
        }
        if (canLoad) {
          command = threadLoadCommandQueue.get(threadId).poll();
          checkNotNull(command);
          if (oocEngine.getOracle().approve(command)) {
            return command;
          } else {
            // Loading is not viable at this moment. We should put the command
            // back in the load queue and wait until loading becomes viable.
            threadLoadCommandQueue.get(threadId).offer(command);
          }
        }
      }
      command = null;
      for (OutOfCoreOracle.IOAction action : actions) {
        Integer partitionId;
        switch (action) {
        case STORE_MESSAGES_AND_BUFFERS:
          partitionId = oocEngine.getMetaPartitionManager()
              .getOffloadPartitionBufferId(threadId);
          if (partitionId != null) {
            command = new StoreDataBufferIOCommand(oocEngine, partitionId,
                StoreDataBufferIOCommand.DataBufferType.PARTITION);
          } else {
            partitionId = oocEngine.getMetaPartitionManager()
                .getOffloadMessageBufferId(threadId);
            if (partitionId != null) {
              command = new StoreDataBufferIOCommand(oocEngine, partitionId,
                  StoreDataBufferIOCommand.DataBufferType.MESSAGE);
            } else {
              partitionId = oocEngine.getMetaPartitionManager()
                  .getOffloadMessageId(threadId);
              if (partitionId != null) {
                command = new StoreIncomingMessageIOCommand(oocEngine,
                    partitionId);
              }
            }
          }
          break;
        case STORE_PROCESSED_PARTITION:
          partitionId = oocEngine.getMetaPartitionManager()
              .getOffloadPartitionId(threadId);
          if (partitionId != null &&
              oocEngine.getMetaPartitionManager()
                  .isPartitionProcessed(partitionId)) {
            command = new StorePartitionIOCommand(oocEngine, partitionId);
          }
          break;
        case STORE_PARTITION:
          partitionId = oocEngine.getMetaPartitionManager()
              .getOffloadPartitionId(threadId);
          if (partitionId != null) {
            command = new StorePartitionIOCommand(oocEngine, partitionId);
          }
          break;
        case LOAD_UNPROCESSED_PARTITION:
          partitionId = oocEngine.getMetaPartitionManager()
              .getLoadPartitionId(threadId);
          if (partitionId != null &&
              !oocEngine.getMetaPartitionManager()
                  .isPartitionProcessed(partitionId)) {
            command = new LoadPartitionIOCommand(oocEngine, partitionId,
                oocEngine.getSuperstep());
          }
          break;
        case LOAD_TO_SWAP_PARTITION:
          partitionId = oocEngine.getMetaPartitionManager()
              .getLoadPartitionId(threadId);
          if (partitionId != null &&
              !oocEngine.getMetaPartitionManager()
                  .isPartitionProcessed(partitionId) &&
              oocEngine.getMetaPartitionManager().hasProcessedOnMemory()) {
            command = new LoadPartitionIOCommand(oocEngine, partitionId,
                oocEngine.getSuperstep());
          }
          break;
        case LOAD_PARTITION:
          partitionId = oocEngine.getMetaPartitionManager()
              .getLoadPartitionId(threadId);
          if (partitionId != null) {
            if (oocEngine.getMetaPartitionManager()
                .isPartitionProcessed(partitionId)) {
              command = new LoadPartitionIOCommand(oocEngine, partitionId,
                  oocEngine.getSuperstep() + 1);
            } else {
              command = new LoadPartitionIOCommand(oocEngine, partitionId,
                  oocEngine.getSuperstep());
            }
          }
          break;
        case URGENT_LOAD_PARTITION:
          // Do nothing
          break;
        default:
          throw new IllegalStateException("getNextIOCommand: the IO action " +
              "is not defined!");
        }
        if (command != null) {
          break;
        }
      }
      if (command == null) {
        command = new WaitIOCommand(oocEngine, waitInterval);
      }
    } while (!oocEngine.getOracle().approve(command));
    return command;
  }

  /**
   * Notify IO scheduler that the IO command is completed
   *
   * @param command completed command
   */
  public void ioCommandCompleted(IOCommand command) {
    oocEngine.ioCommandCompleted(command);
  }

  /**
   * Add an IO command to the scheduling queue of the IO scheduler
   *
   * @param ioCommand IO command to add to the scheduler
   */
  public void addIOCommand(IOCommand ioCommand) {
    if (ioCommand instanceof LoadPartitionIOCommand) {
      int ownerThread = oocEngine.getMetaPartitionManager()
          .getOwnerThreadId(ioCommand.getPartitionId());
      threadLoadCommandQueue.get(ownerThread).offer(ioCommand);
    } else {
      throw new IllegalStateException("addIOCommand: IO command type is not " +
          "supported for addition");
    }
  }

  /**
   * Shutdown/Terminate the IO scheduler, and notify all IO threads to halt
   */
  public void shutdown() {
    shouldTerminate = true;
    if (LOG.isInfoEnabled()) {
      LOG.info("shutdown: OutOfCoreIOScheduler shutting down!");
    }
  }
}
