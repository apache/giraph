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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.ooc.data.MetaPartitionManager;
import org.apache.giraph.ooc.io.IOCommand;
import org.apache.giraph.ooc.io.LoadPartitionIOCommand;
import org.apache.giraph.ooc.io.StorePartitionIOCommand;
import org.apache.giraph.ooc.io.WaitIOCommand;
import org.apache.log4j.Logger;

/**
 * Out-of-core engine maintaining fixed number of partitions in memory.
 */
public class FixedOutOfCoreEngine extends OutOfCoreEngine {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(FixedOutOfCoreEngine.class);
  /**
   * When getting partitions, how many milliseconds to wait if no partition was
   * available in memory
   */
  private static final long MSEC_TO_WAIT = 1000;
  /**
   * Dummy object to wait on until a partition becomes available in memory
   * for processing
   */
  private final Object partitionAvailable = new Object();

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param service Service worker
   * @param maxPartitionsInMemory Maximum number of partitions that can be kept
   *                              in memory
   */
  public FixedOutOfCoreEngine(ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
                              CentralizedServiceWorker<?, ?, ?> service,
                              int maxPartitionsInMemory) {
    super(conf, service);
    this.ioScheduler = new FixedOutOfCoreIOScheduler(maxPartitionsInMemory,
        numIOThreads, this, conf);
  }

  @Override
  public Integer getNextPartition() {
    Integer partitionId;
    synchronized (partitionAvailable) {
      while ((partitionId = metaPartitionManager.getNextPartition()) == null) {
        try {
          if (LOG.isInfoEnabled()) {
            LOG.info("getNextPartition: waiting until a partition becomes " +
                "available!");
          }
          partitionAvailable.wait(MSEC_TO_WAIT);
        } catch (InterruptedException e) {
          throw new IllegalStateException("getNextPartition: caught " +
              "InterruptedException while waiting to retrieve a partition to " +
              "process");
        }
        if (jobFailed) {
          throw new RuntimeException("Job Failed due to a failure in an " +
              "out-of-core IO thread");
        }
      }
    }
    if (partitionId == MetaPartitionManager.NO_PARTITION_TO_PROCESS) {
      partitionId = null;
    }
    return partitionId;
  }

  @Override
  public void doneProcessingPartition(int partitionId) {
    metaPartitionManager.setPartitionIsProcessed(partitionId);
    // Put the partition in store IO command queue and announce this partition
    // as a candidate to offload to disk.
    if (LOG.isInfoEnabled()) {
      LOG.info("doneProcessingPartition: processing partition " + partitionId +
          " is done!");
    }
    ioScheduler.addIOCommand(new StorePartitionIOCommand(this, partitionId));
  }

  @Override
  public void startIteration() {
    getSuperstepLock().writeLock().lock();
    metaPartitionManager.resetPartition();
    ((FixedOutOfCoreIOScheduler) ioScheduler).clearStoreCommandQueue();
    getSuperstepLock().writeLock().unlock();
  }

  @Override
  public void retrievePartition(int partitionId) {
    long superstep = service.getSuperstep();
    if (metaPartitionManager.isPartitionOnDisk(partitionId)) {
      ioScheduler.addIOCommand(new LoadPartitionIOCommand(this, partitionId,
          superstep));
      synchronized (partitionAvailable) {
        while (metaPartitionManager.isPartitionOnDisk(partitionId)) {
          try {
            if (LOG.isInfoEnabled()) {
              LOG.info("retrievePartition: waiting until partition " +
                  partitionId + " becomes available");
            }
            partitionAvailable.wait();
          } catch (InterruptedException e) {
            throw new IllegalStateException("retrievePartition: caught " +
                "InterruptedException while waiting to retrieve partition " +
                partitionId);
          }
        }
      }
    }
  }

  @Override
  public void ioCommandCompleted(IOCommand command) {
    if (command instanceof LoadPartitionIOCommand ||
        command instanceof WaitIOCommand) {
      // Notifying compute threads who are waiting for a partition to become
      // available in memory to process.
      synchronized (partitionAvailable) {
        partitionAvailable.notifyAll();
      }
    }
  }
}
