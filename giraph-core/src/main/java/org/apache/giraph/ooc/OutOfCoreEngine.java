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
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.ooc.data.MetaPartitionManager;
import org.apache.giraph.ooc.io.IOCommand;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * Class to represent an out-of-core engine.
 */
public abstract class OutOfCoreEngine {
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(OutOfCoreEngine.class);
  /** Service worker */
  protected final CentralizedServiceWorker<?, ?, ?> service;
  /** Scheduler for IO threads */
  protected OutOfCoreIOScheduler ioScheduler;
  /** Data structure to keep meta partition information */
  protected final MetaPartitionManager metaPartitionManager;
  /** How many disk (i.e. IO threads) do we have? */
  protected final int numIOThreads;
  /**
   * Whether the job should fail due to IO threads terminating because of
   * exceptions
   */
  protected volatile boolean jobFailed = false;
  /** Whether the out-of-core engine has initialized */
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  /**
   * Global lock for entire superstep. This lock helps to avoid overlapping of
   * out-of-core decisions (what to do next to help the out-of-core mechanism)
   * with out-of-core operations (actual IO operations).
   */
  private final ReadWriteLock superstepLock = new ReentrantReadWriteLock();
  /** Callable factory for IO threads */
  private final OutOfCoreIOCallableFactory oocIOCallableFactory;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param service Service worker
   */
  public OutOfCoreEngine(ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
                         CentralizedServiceWorker<?, ?, ?> service) {
    this.service = service;
    this.oocIOCallableFactory = new OutOfCoreIOCallableFactory(conf, this);
    this.numIOThreads = oocIOCallableFactory.getNumDisks();
    this.metaPartitionManager = new MetaPartitionManager(numIOThreads, this);
    oocIOCallableFactory.createCallable();
  }

  /**
   * Initialize/Start the out-of-core engine.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      "JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void initialize() {
    synchronized (isInitialized) {
      isInitialized.set(true);
      isInitialized.notifyAll();
    }
  }

  /**
   * Shutdown/Stop the out-of-core engine.
   */
  public void shutdown() {
    if (LOG.isInfoEnabled()) {
      LOG.info("shutdown: out-of-core engine shutting down, signalling IO " +
          "threads to shutdown");
    }
    ioScheduler.shutdown();
    oocIOCallableFactory.shutdown();
  }

  /**
   * Get a reference to the server data
   *
   * @return ServerData
   */
  public ServerData getServerData() {
    return service.getServerData();
  }

  /**
   * Get a reference to the service worker
   *
   * @return CentralizedServiceWorker
   */
  public CentralizedServiceWorker getServiceWorker() {
    return service;
  }

  /**
   * Get a reference to IO scheduler
   *
   * @return OutOfCoreIOScheduler
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      "JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public OutOfCoreIOScheduler getIOScheduler() {
    synchronized (isInitialized) {
      while (!isInitialized.get()) {
        try {
          isInitialized.wait();
        } catch (InterruptedException e) {
          throw new IllegalStateException("getIOScheduler: " +
              "InterruptedException while waiting for out-of-core engine to " +
              "be initialized!");
        }
      }
    }
    return ioScheduler;
  }

  /**
   * Get a reference to meta partition information
   *
   * @return MetaPartitionManager
   */
  public MetaPartitionManager getMetaPartitionManager() {
    checkState(isInitialized.get());
    return metaPartitionManager;
  }

  /**
   * Get a refernce to superstep lock
   *
   * @return read/write lock used for global superstep lock
   */
  public ReadWriteLock getSuperstepLock() {
    return superstepLock;
  }

  /**
   * Get the id of the next partition to process in the current iteration over
   * all the partitions. If all partitions are already processed, this method
   * returns null.
   *
   * @return id of a partition to process. 'null' if all partitions are
   *         processed in current iteration over partitions.
   */
  public abstract Integer getNextPartition();

  /**
   * Notify out-of-core engine that processing of a particular partition is done
   *
   * @param partitionId id of the partition that its processing is done
   */
  public abstract void doneProcessingPartition(int partitionId);

  /**
   * Notify out=of-core engine that iteration cycle over all partitions is about
   * to begin.
   */
  public abstract void startIteration();

  /**
   * Retrieve a particular partition. After this method is complete the
   * requested partition should be in memory.
   *
   * @param partitionId id of the partition to retrieve
   */
  public abstract void retrievePartition(int partitionId);

  /**
   * Notify out-of-core engine that an IO command is competed by an IO thread
   *
   * @param command the IO command that is completed
   */
  public abstract void ioCommandCompleted(IOCommand command);

  /**
   * Set a flag to fail the job.
   */
  public void failTheJob() {
    jobFailed = true;
  }
}
