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
import org.apache.giraph.ooc.command.IOCommand;

/**
 * Interface for any out-of-core oracle. An out-of-core oracle is the brain of
 * the out-of-core mechanism, determining/deciding on out-of-core actions (load
 * or store) that should happen.
 * Note: any class implementing this interface should have one and only one
 *       constructor taking only two arguments of types
 *       <code>ImmutableClassesGiraphConfiguration</code> and
 *       <code>OutOfCoreEngine</code>
 */
public interface OutOfCoreOracle {
  /**
   * Different types of IO actions that can potentially lead to a more desired
   * state of computation for out-of-core mechanism. These actions are issued
   * based on the status of the memory (memory pressure, rate of data transfer
   * to memory, etc.)
   */
  enum IOAction {
    /**
     * Either of:
     *    - storing incoming messages of any partition currently on disk, or
     *    - storing incoming messages' raw data buffer of any partition
     *      currently on disk, or
     *    - storing partitions' raw data buffer for those partitions that are
     *      currently on disk.
     */
    STORE_MESSAGES_AND_BUFFERS,
    /**
     * Storing a partition that is *processed* in the current iteration cycle.
     * This action is also known as "soft store"
     */
    STORE_PROCESSED_PARTITION,
    /**
     * Storing a partition from memory on disk, prioritizing to *processed*
     * partitions on memory. However, if there is no *processed* partition,
     * store should happen at any cost, even if an *unprocessed* partition has
     * to be stored. This action is also know as "hard store".
     */
    STORE_PARTITION,
    /**
     * Loading an *unprocessed* partition from disk to memory, only if there are
     * *processed* partitions in memory. This action basically initiates a swap
     * operation.
     */
    LOAD_TO_SWAP_PARTITION,
    /**
     * Loading an *unprocessed* partition from disk to memory. This action is
     * also known as "soft load".
     */
    LOAD_UNPROCESSED_PARTITION,
    /**
     * Loading a partition (prioritizing *unprocessed* over *processed*) from
     * disk to memory. Loading a *processed* partition to memory is a prefetch
     * of that partition to be processed in the next superstep. This action is
     * also known as "hard load".
     */
    LOAD_PARTITION,
    /**
     * Loading a partition regardless of the memory situation. An out-of-core
     * mechanism may use this action to signal IO threads that it is allowed to
     * load a partition that is specifically requested.
     */
    URGENT_LOAD_PARTITION
  }

  /**
   * Get the next set of viable IO actions to help bring memory to a more
   * desired state.
   *
   * @return an array of viable IO actions, sorted from highest priority to
   *         lowest priority
   */
  IOAction[] getNextIOActions();

  /**
   * Whether a command is appropriate to bring the memory to a more desired
   * state. A command is not executed unless it is approved by the oracle. This
   * method is specially important where there are multiple IO threads
   * performing IO operations for the out-of-core mechanism. The approval
   * becomes significantly important to prevent all IO threads from performing
   * identical command type, if that is a necessity. For instance, execution of
   * a particular command type by only one thread may bring the memory to a
   * desired state, and the rest of IO threads may perform other types of
   * commands.
   *
   * @param command the IO command that is about to execute
   * @return 'true' if the command is approved for execution. 'false' if the
   *         command should not be executed
   */
  boolean approve(IOCommand command);

  /**
   * Notification of command completion. Oracle may update its status and commit
   * the changes a command may cause.
   *
   * @param command the IO command that is completed
   */
  void commandCompleted(IOCommand command);

  /**
   * Notification of GC completion. Oracle may take certain decisions based on
   * GC information (such as amount of time it took, memory it reclaimed, etc.)
   *
   * @param gcInfo GC information
   */
  void gcCompleted(GarbageCollectionNotificationInfo gcInfo);

  /**
   * Called at the beginning of a superstep.
   */
  void startIteration();
}
