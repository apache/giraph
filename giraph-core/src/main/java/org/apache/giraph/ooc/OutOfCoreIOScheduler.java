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

import com.google.common.hash.Hashing;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.ooc.io.IOCommand;
import org.apache.log4j.Logger;

/**
 * Representation of IO thread scheduler for out-of-core mechanism
 */
public abstract class OutOfCoreIOScheduler {
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
  protected final OutOfCoreEngine oocEngine;
  /** How much an IO thread should wait if there is no IO command */
  protected final int waitInterval;
  /** How many disks (i.e. IO threads) do we have? */
  private final int numDisks;

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
    this.numDisks = numDisks;
    this.waitInterval = OOC_WAIT_INTERVAL.get(conf);
  }

  /**
   * Get the thread id that is responsible for a particular partition
   *
   * @param partitionId id of the given partition
   * @return id of the thread responsible for the given partition
   */
  public int getOwnerThreadId(int partitionId) {
    int result = Hashing.murmur3_32().hashInt(partitionId).asInt() % numDisks;
    return (result >= 0) ? result : (result + numDisks);
  }

  /**
   * Generate and return the next appropriate IO command for a given thread
   *
   * @param threadId id of the thread ready to execute the next IO command
   * @return next IO command to be executed by the given thread
   */
  public abstract IOCommand getNextIOCommand(int threadId);

  /**
   * Notify IO scheduler that the IO command is completed
   *
   * @param command completed command
   */
  public abstract void ioCommandCompleted(IOCommand command);

  /**
   * Add an IO command to the scheduling queue of the IO scheduler
   *
   * @param ioCommand IO command to add to the scheduler
   */
  public abstract void addIOCommand(IOCommand ioCommand);

  /**
   * Shutdown/Terminate the IO scheduler, and notify all IO threads to halt
   */
  public void shutdown() {
    if (LOG.isInfoEnabled()) {
      LOG.info("shutdown: OutOfCoreIOScheduler shutting down!");
    }
  }
}
