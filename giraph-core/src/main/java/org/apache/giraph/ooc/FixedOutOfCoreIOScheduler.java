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
import org.apache.giraph.ooc.io.IOCommand;
import org.apache.giraph.ooc.io.LoadPartitionIOCommand;
import org.apache.giraph.ooc.io.StoreDataBufferIOCommand;
import org.apache.giraph.ooc.io.StoreIncomingMessageIOCommand;
import org.apache.giraph.ooc.io.StorePartitionIOCommand;
import org.apache.giraph.ooc.io.WaitIOCommand;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/**
 * IO Scheduler for out-of-core mechanism with fixed number of partitions in
 * memory
 */
public class FixedOutOfCoreIOScheduler extends OutOfCoreIOScheduler {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(FixedOutOfCoreIOScheduler.class);
  /** Maximum number of partitions to be kept in memory */
  private final int maxPartitionsInMemory;
  /**
   * Number of partitions to be added (loaded) or removed (stored) to/from
   * memory. Each outstanding load partition counts +1 and each outstanding
   * store partition counts -1 toward this counter.
   */
  private final AtomicInteger deltaNumPartitionsInMemory =
      new AtomicInteger(0);
  /** Queue of IO commands for loading partitions to memory */
  private final List<Queue<IOCommand>> threadLoadCommandQueue;
  /** Queue of IO commands for storing partition on disk */
  private final List<Queue<IOCommand>> threadStoreCommandQueue;
  /** Whether IO threads should terminate */
  private volatile boolean shouldTerminate;

  /**
   * Constructor
   *  @param maxPartitionsInMemory maximum number of partitions can be kept in
   *                              memory
   * @param numThreads number of available IO threads (i.e. disks)
   * @param oocEngine out-of-core engine
   * @param conf configuration
   */
  public FixedOutOfCoreIOScheduler(int maxPartitionsInMemory, int numThreads,
                                   OutOfCoreEngine oocEngine,
                                   ImmutableClassesGiraphConfiguration conf) {
    super(conf, oocEngine, numThreads);
    this.maxPartitionsInMemory = maxPartitionsInMemory;
    threadLoadCommandQueue = new ArrayList<>(numThreads);
    threadStoreCommandQueue = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      threadLoadCommandQueue.add(
          new ConcurrentLinkedQueue<IOCommand>());
      threadStoreCommandQueue.add(
          new ConcurrentLinkedQueue<IOCommand>());
    }
    shouldTerminate = false;
  }

  @Override
  public IOCommand getNextIOCommand(int threadId) {
    if (shouldTerminate) {
      return null;
    }
    int numPartitionsInMemory =
        oocEngine.getMetaPartitionManager().getNumInMemoryPartitions();
    IOCommand command = null;
    if (LOG.isInfoEnabled()) {
      LOG.info("getNextIOCommand with " + numPartitionsInMemory +
          " partitions in memory, " + deltaNumPartitionsInMemory.get() +
          " on the fly");
    }
    // Check if we have to store a partition on disk
    if (numPartitionsInMemory + deltaNumPartitionsInMemory.getAndDecrement() >
        maxPartitionsInMemory) {
      command = threadStoreCommandQueue.get(threadId).poll();
      if (command == null) {
        Integer partitionId = oocEngine.getMetaPartitionManager()
            .getOffloadPartitionId(threadId);
        if (partitionId != null) {
          command = new StorePartitionIOCommand(oocEngine, partitionId);
        } else {
          deltaNumPartitionsInMemory.getAndIncrement();
        }
      } else {
        checkState(command instanceof StorePartitionIOCommand,
            "getNextIOCommand: Illegal command type in store command queue!");
      }
    } else {
      // Roll back the decrement in delta counter.
      deltaNumPartitionsInMemory.getAndIncrement();
    }

    // Check if there is any buffers/messages of current out-of-core partitions
    // in memory
    if (command == null) {
      Integer partitionId = oocEngine.getMetaPartitionManager()
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
            command = new StoreIncomingMessageIOCommand(oocEngine, partitionId);
          }
        }
      }
    }

    // Check if we can load/prefetch a partition to memory
    if (command == null) {
      if (numPartitionsInMemory +
          deltaNumPartitionsInMemory.getAndIncrement() <=
          maxPartitionsInMemory) {
        command = threadLoadCommandQueue.get(threadId).poll();
        if (command == null) {
          Integer partitionId = oocEngine.getMetaPartitionManager()
              .getPrefetchPartitionId(threadId);
          if (partitionId != null) {
            command = new LoadPartitionIOCommand(oocEngine, partitionId,
                oocEngine.getServiceWorker().getSuperstep());
          } else {
            deltaNumPartitionsInMemory.getAndDecrement();
          }
        }
      } else {
        // Roll back the increment in delta counter.
        deltaNumPartitionsInMemory.getAndDecrement();
      }
    }

    // Check if no appropriate IO command is found
    if (command == null) {
      command = new WaitIOCommand(oocEngine, waitInterval);
    }
    return command;
  }

  @Override
  public void ioCommandCompleted(IOCommand command) {
    if (command instanceof LoadPartitionIOCommand) {
      deltaNumPartitionsInMemory.getAndDecrement();
    } else if (command instanceof StorePartitionIOCommand) {
      deltaNumPartitionsInMemory.getAndIncrement();
    }
    oocEngine.ioCommandCompleted(command);
  }

  @Override
  public void addIOCommand(IOCommand ioCommand) {
    int ownerThread = getOwnerThreadId(ioCommand.getPartitionId());
    if (ioCommand instanceof LoadPartitionIOCommand) {
      threadLoadCommandQueue.get(ownerThread).offer(ioCommand);
    } else if (ioCommand instanceof StorePartitionIOCommand) {
      threadStoreCommandQueue.get(ownerThread).offer(ioCommand);
    } else {
      throw new IllegalStateException("addIOCommand: IO command type is not " +
          "supported for addition");
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    shouldTerminate = true;
  }

  /**
   * Clear store command queue (should happen at the beginning of each iteration
   * to eliminate eager store commands generated by OOC engine)
   */
  public void clearStoreCommandQueue() {
    for (int i = 0; i < threadStoreCommandQueue.size(); ++i) {
      threadStoreCommandQueue.get(i).clear();
    }
  }
}
