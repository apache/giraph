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

package org.apache.giraph.ooc.policy;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.command.StorePartitionIOCommand;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/** Oracle for fixed out-of-core mechanism */
public class FixedPartitionsOracle implements OutOfCoreOracle {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(FixedPartitionsOracle.class);
  /** Maximum number of partitions to be kept in memory */
  private final int maxPartitionsInMemory;
  /**
   * Number of partitions to be added (loaded) or removed (stored) to/from
   * memory. Each outstanding load partition counts +1 and each outstanding
   * store partition counts -1 toward this counter.
   */
  private final AtomicInteger deltaNumPartitionsInMemory =
      new AtomicInteger(0);
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param oocEngine out-of-core engine
   */
  public FixedPartitionsOracle(ImmutableClassesGiraphConfiguration conf,
                               OutOfCoreEngine oocEngine) {
    this.maxPartitionsInMemory =
        GiraphConstants.MAX_PARTITIONS_IN_MEMORY.get(conf);
    this.oocEngine = oocEngine;
  }

  @Override
  public IOAction[] getNextIOActions() {
    int numPartitionsInMemory =
        oocEngine.getMetaPartitionManager().getNumInMemoryPartitions();
    int numPartialPartitionsInMemory =
        oocEngine.getMetaPartitionManager().getNumPartiallyInMemoryPartitions();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getNextIOActions: calling with " + numPartitionsInMemory +
          " partitions entirely in memory and " + numPartialPartitionsInMemory +
          " partitions partially in memory, " +
          deltaNumPartitionsInMemory.get() + " to be loaded");
    }
    checkState(numPartitionsInMemory >= 0);
    checkState(numPartialPartitionsInMemory >= 0);
    int numPartitions =
        numPartitionsInMemory + deltaNumPartitionsInMemory.get();
    // Fixed out-of-core policy:
    //   - if the number of partitions in memory is less than the max number of
    //     partitions in memory, we should load a partition to memory. This
    //     basically means we are prefetching partition's data either for the
    //     current superstep, or for the next superstep.
    //   - if the number of partitions in memory is equal to the the max number
    //     of partitions in memory, we do a 'soft store', meaning, we store
    //     processed partition to disk only if there is an unprocessed partition
    //     on disk. This basically makes room for unprocessed partitions on disk
    //     to be prefetched.
    //   - if the number of partitions in memory is more than the max number of
    //     partitions in memory, we do a 'hard store', meaning we store a
    //     partition to disk, regardless of its processing state.
    if (numPartitions < maxPartitionsInMemory) {
      return new IOAction[]{
        IOAction.LOAD_PARTITION,
        IOAction.STORE_MESSAGES_AND_BUFFERS};
    } else if (numPartitions > maxPartitionsInMemory) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getNextIOActions: number of partitions in memory passed " +
          "the specified threshold!");
      }
      return new IOAction[]{
        IOAction.STORE_PARTITION,
        IOAction.STORE_MESSAGES_AND_BUFFERS};
    } else {
      return new IOAction[]{
        IOAction.STORE_MESSAGES_AND_BUFFERS,
        IOAction.LOAD_TO_SWAP_PARTITION};
    }
  }

  @Override
  public boolean approve(IOCommand command) {
    int numPartitionsInMemory = oocEngine.getMetaPartitionManager()
        .getNumInMemoryPartitions();
    // If loading a partition result in having more partition in memory, the
    // command should be denied. Also, if number of partitions in memory is
    // already less than the max number of partitions, any command for storing
    // a partition should be denied.
    if (command instanceof LoadPartitionIOCommand &&
        numPartitionsInMemory + deltaNumPartitionsInMemory.getAndIncrement() >
            maxPartitionsInMemory) {
      deltaNumPartitionsInMemory.getAndDecrement();
      return false;

    } else if (command instanceof StorePartitionIOCommand &&
        numPartitionsInMemory + deltaNumPartitionsInMemory.getAndDecrement() <
            maxPartitionsInMemory) {
      deltaNumPartitionsInMemory.getAndIncrement();
      return false;
    }
    return true;
  }

  @Override
  public void commandCompleted(IOCommand command) {
    if (command instanceof LoadPartitionIOCommand) {
      deltaNumPartitionsInMemory.getAndDecrement();
    } else if (command instanceof StorePartitionIOCommand) {
      deltaNumPartitionsInMemory.getAndIncrement();
    }
  }

  @Override
  public void gcCompleted(GarbageCollectionNotificationInfo gcInfo) { }

  @Override
  public void startIteration() {
  }
}
