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

import com.sun.management.GarbageCollectionNotificationInfo;
import com.yammer.metrics.core.Gauge;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.NetworkMetrics;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.flow_control.CreditBasedFlowControl;
import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.ooc.data.MetaPartitionManager;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.giraph.ooc.policy.FixedPartitionsOracle;
import org.apache.giraph.ooc.policy.OutOfCoreOracle;
import org.apache.giraph.utils.AdjustableSemaphore;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * Class to represent an out-of-core engine.
 */
public class OutOfCoreEngine implements ResetSuperstepMetricsObserver {
  /**
   * Number of 'units of processing' after which an active thread should
   * check-in with the out-of-core engine in order to re-claim its permission to
   * stay active. For a compute thread, the 'unit of processing' is processing
   * of one vertex, and for an input thread, the 'unit of processing' is reading
   * a row of input data.
   */
  public static final int CHECK_IN_INTERVAL = (1 << 10) - 1;
  /** Name of metric for percentage of graph on disk */
  public static final String GRAPH_PERCENTAGE_IN_MEMORY = "ooc-graph-in-mem-%";
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(OutOfCoreEngine.class);
  /**
   * When getting partitions, how many milliseconds to wait if no partition was
   * available in memory
   */
  private static final long MSEC_TO_WAIT = 10000;
  /** Service worker */
  private final CentralizedServiceWorker<?, ?, ?> service;
  /** Flow control used in sending requests */
  private FlowControl flowControl;
  /** Scheduler for IO threads */
  private final OutOfCoreIOScheduler ioScheduler;
  /** Data structure to keep meta partition information */
  private final MetaPartitionManager metaPartitionManager;
  /** Out-of-core oracle (brain of out-of-core mechanism) */
  private final OutOfCoreOracle oracle;
  /** IO statistics collector */
  private final OutOfCoreIOStatistics statistics;
  /**
   * Global lock for entire superstep. This lock helps to avoid overlapping of
   * out-of-core decisions (what to do next to help the out-of-core mechanism)
   * with out-of-core operations (actual IO operations).
   */
  private final ReadWriteLock superstepLock = new ReentrantReadWriteLock();
  /** Data accessor object (DAO) used as persistence layer in out-of-core */
  private final OutOfCoreDataAccessor dataAccessor;
  /** Callable factory for IO threads */
  private final OutOfCoreIOCallableFactory oocIOCallableFactory;
  /**
   * Dummy object to wait on until a partition becomes available in memory
   * for processing
   */
  private final Object partitionAvailable = new Object();
  /** How many compute threads do we have? */
  private int numComputeThreads;
  /** How many threads (input/compute) are processing data? */
  private volatile int numProcessingThreads;
  /** Semaphore used for controlling number of active threads at each moment */
  private final AdjustableSemaphore activeThreadsPermit;
  /**
   * Cached value for NettyClient.MAX_NUM_OF_OPEN_REQUESTS_PER_WORKER (max
   * credit used for credit-based flow-control mechanism)
   */
  private final short maxRequestsCredit;
  /**
   * Generally, the logic in Giraph for change of the superstep happens in the
   * following order:
   *   (1) Compute threads are done processing all partitions
   *   (2) Superstep number increases
   *   (3) New message store is created and message stores are prepared
   *   (4) Iteration over partitions starts
   * Note that there are other operations happening at the same time as well as
   * the above operations, but the above operations are the ones which may
   * interfere with out-of-core operations. The goal of `superstepLock` is to
   * isolate operations 2, 3, and 4 from the rest of computations and IO
   * operations. Specifically, increasing the superstep counter (operation 2)
   * should be exclusive and no IO operation should happen at the same time.
   * This is due to the fact that prefetching mechanism uses superstep counter
   * as a mean to identify which data should be read. That being said, superstep
   * counter should be cached in out-of-core engine, and all IO operations and
   * out-of-core logic should access superstep counter through this cached
   * value.
   */
  private long superstep;
  /**
   * Generally, the logic of a graph computations happens in the following order
   * with respect to `startIteration` and `reset` method:
   * ...
   * startIteration (for moving edges)
   * ...
   * reset (to prepare messages/partitions for superstep 0)
   * ...
   * startIteration (superstep 0)
   * ...
   * reset (to prepare messages/partitions for superstep 1)
   * ...
   *
   * However, in the unit tests, we usually consider only one superstep (usually
   * INPUT_SUPERSTEP), and we move through partitions multiple times. Out-of-
   * core mechanism works only if partitions are reset in a proper way. So,
   * we keep the following flag to reset partitions if necessary.
   */
  private boolean resetDone;

  /**
   * Provides statistics about network traffic (e.g. received bytes per
   * superstep etc).
   */
  private final NetworkMetrics networkMetrics;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param service Service worker
   * @param networkMetrics Interface for network stats
   */
  public OutOfCoreEngine(ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
                         CentralizedServiceWorker<?, ?, ?> service,
                         NetworkMetrics networkMetrics) {
    this.service = service;
    this.networkMetrics = networkMetrics;
    Class<? extends OutOfCoreDataAccessor> accessorClass =
        GiraphConstants.OUT_OF_CORE_DATA_ACCESSOR.get(conf);
    try {
      Constructor<?> constructor = accessorClass.getConstructor(
          ImmutableClassesGiraphConfiguration.class);
      this.dataAccessor = (OutOfCoreDataAccessor) constructor.newInstance(conf);
    } catch (NoSuchMethodException | InstantiationException |
        InvocationTargetException | IllegalAccessException e) {
      throw new IllegalStateException("OutOfCoreEngine: caught exception " +
          "while creating the data accessor instance!", e);
    }
    int numIOThreads = dataAccessor.getNumAccessorThreads();
    this.oocIOCallableFactory =
        new OutOfCoreIOCallableFactory(this, numIOThreads,
            service.getGraphTaskManager().createUncaughtExceptionHandler());
    this.ioScheduler = new OutOfCoreIOScheduler(conf, this, numIOThreads);
    this.metaPartitionManager = new MetaPartitionManager(numIOThreads, this);
    this.statistics = new OutOfCoreIOStatistics(conf, numIOThreads);
    int maxPartitionsInMemory =
        GiraphConstants.MAX_PARTITIONS_IN_MEMORY.get(conf);
    Class<? extends OutOfCoreOracle> oracleClass =
        GiraphConstants.OUT_OF_CORE_ORACLE.get(conf);
    if (maxPartitionsInMemory != 0 &&
        oracleClass != FixedPartitionsOracle.class) {
      LOG.warn("OutOfCoreEngine: Max number of partitions in memory is set " +
          "but the out-of-core oracle used is not tailored for fixed " +
          "out-of-core policy. Setting the oracle to be FixedPartitionsOracle");
      oracleClass = FixedPartitionsOracle.class;
    }
    this.numComputeThreads = conf.getNumComputeThreads();
    // At the beginning of the execution, only input threads are processing data
    this.numProcessingThreads = conf.getNumInputSplitsThreads();
    this.activeThreadsPermit = new AdjustableSemaphore(numProcessingThreads);
    this.maxRequestsCredit = (short)
        CreditBasedFlowControl.MAX_NUM_OF_OPEN_REQUESTS_PER_WORKER.get(conf);
    this.superstep = BspService.INPUT_SUPERSTEP;
    this.resetDone = false;
    GiraphMetrics.get().addSuperstepResetObserver(this);
    try {
      Constructor<?> constructor = oracleClass.getConstructor(
        ImmutableClassesGiraphConfiguration.class, OutOfCoreEngine.class);
      this.oracle = (OutOfCoreOracle) constructor.newInstance(conf, this);
    } catch (NoSuchMethodException | IllegalAccessException |
      InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException("OutOfCoreEngine: caught exception " +
        "while creating the oracle!", e);
    }
  }

  /**
   * Initialize/Start the out-of-core engine.
   */
  public void initialize() {
    dataAccessor.initialize();
    oocIOCallableFactory.createCallable();
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
    dataAccessor.shutdown();
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
  public OutOfCoreIOScheduler getIOScheduler() {
    return ioScheduler;
  }

  /**
   * Get a reference to meta partition information
   *
   * @return MetaPartitionManager
   */
  public MetaPartitionManager getMetaPartitionManager() {
    return metaPartitionManager;
  }

  /**
   * Get a reference to superstep lock
   *
   * @return read/write lock used for global superstep lock
   */
  public ReadWriteLock getSuperstepLock() {
    return superstepLock;
  }

  /**
   * Get a reference to IO statistics collector
   *
   * @return IO statistics collector
   */
  public OutOfCoreIOStatistics getIOStatistics() {
    return statistics;
  }

  /**
   * Get a reference to out-of-core oracle
   *
   * @return out-of-core oracle
   */
  public OutOfCoreOracle getOracle() {
    return oracle;
  }

  /**
   * Get the id of the next partition to process in the current iteration over
   * all the partitions. If all partitions are already processed, this method
   * returns null.
   *
   * @return id of a partition to process. 'null' if all partitions are
   *         processed in current iteration over partitions.
   */
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
      }
      if (partitionId == MetaPartitionManager.NO_PARTITION_TO_PROCESS) {
        partitionAvailable.notifyAll();
        partitionId = null;
      }
    }
    return partitionId;
  }

  /**
   * Notify out-of-core engine that processing of a particular partition is done
   *
   * @param partitionId id of the partition that its processing is done
   */
  public void doneProcessingPartition(int partitionId) {
    metaPartitionManager.setPartitionIsProcessed(partitionId);
    if (LOG.isInfoEnabled()) {
      LOG.info("doneProcessingPartition: processing partition " + partitionId +
          " is done!");
    }
  }

  /**
   * Notify out-of-core engine that iteration cycle over all partitions is about
   * to begin.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      "UL_UNRELEASED_LOCK_EXCEPTION_PATH")
  public void startIteration() {
    oracle.startIteration();
    if (!resetDone) {
      superstepLock.writeLock().lock();
      metaPartitionManager.resetPartitions();
      superstepLock.writeLock().unlock();
    }
    if (superstep != BspServiceWorker.INPUT_SUPERSTEP &&
        numProcessingThreads != numComputeThreads) {
      // This method is only executed by the main thread, and at this point
      // no other input/compute thread is alive. So, all the permits in
      // `activeThreadsPermit` is available. However, now that we are changing
      // the maximum number of active threads, we need to adjust the number
      // of available permits on `activeThreadsPermit`.
      activeThreadsPermit.setMaxPermits(activeThreadsPermit.availablePermits() *
          numComputeThreads / numProcessingThreads);
      numProcessingThreads = numComputeThreads;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("startIteration: with " +
          metaPartitionManager.getNumInMemoryPartitions() +
          " partitions in memory and " +
          activeThreadsPermit.availablePermits() + " active threads");
    }
    resetDone = false;
  }

  /**
   * Retrieve a particular partition. After this method is complete the
   * requested partition should be in memory.
   *
   * @param partitionId id of the partition to retrieve
   */
  public void retrievePartition(int partitionId) {
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

  /**
   * Notify out-of-core engine that an IO command is completed by an IO thread
   *
   * @param command the IO command that is completed
   */
  public void ioCommandCompleted(IOCommand command) {
    oracle.commandCompleted(command);
    if (command instanceof LoadPartitionIOCommand) {
      // Notifying compute threads who are waiting for a partition to become
      // available in memory to process.
      synchronized (partitionAvailable) {
        partitionAvailable.notifyAll();
      }
    }
  }

  /**
   * Update the fraction of processing threads that should remain active. It is
   * the responsibility of out-of-core oracle to update the number of active
   * threads.
   *
   * @param fraction the fraction of processing threads to remain active. This
   *                 number is in range [0, 1]
   */
  public void updateActiveThreadsFraction(double fraction) {
    checkState(fraction >= 0 && fraction <= 1);
    int numActiveThreads = (int) (numProcessingThreads * fraction);
    if (LOG.isInfoEnabled()) {
      LOG.info("updateActiveThreadsFraction: updating the number of active " +
          "threads to " + numActiveThreads);
    }
    activeThreadsPermit.setMaxPermits(numActiveThreads);
  }

  /**
   * A processing thread would check in with out-of-core engine every once in a
   * while to make sure that it can still remain active. It is the
   * responsibility of the out-of-core oracle to update the number of active
   * threads in a way that the computation never fails, and yet achieve the
   * optimal performance it can achieve.
   */
  public void activeThreadCheckIn() {
    activeThreadsPermit.release();
    try {
      activeThreadsPermit.acquire();
    } catch (InterruptedException e) {
      LOG.error("activeThreadCheckIn: exception while acquiring a permit to " +
          "remain an active thread");
      throw new IllegalStateException(e);
    }
  }

  /**
   * Notify the out-of-core engine that a processing (input/compute) thread has
   * started.
   */
  public void processingThreadStart() {
    try {
      activeThreadsPermit.acquire();
    } catch (InterruptedException e) {
      LOG.error("processingThreadStart: exception while acquiring a permit to" +
          " start the processing thread!");
      throw new IllegalStateException(e);
    }
  }

  /**
   * Notify the out-of-core engine that a processing (input/compute) thread has
   * finished.
   */
  public void processingThreadFinish() {
    activeThreadsPermit.release();
  }

  /**
   * Update the credit announced for this worker in Netty. The lower the credit
   * is, the lower rate incoming messages arrive at this worker. Thus, credit
   * is an indirect way of controlling amount of memory incoming messages would
   * take.
   *
   * @param fraction the fraction of max credits others can use to send requests
   *                 to this worker
   */
  public void updateRequestsCreditFraction(double fraction) {
    checkState(fraction >= 0 && fraction <= 1);
    short newCredit = (short) (maxRequestsCredit * fraction);
    if (LOG.isInfoEnabled()) {
      LOG.info("updateRequestsCreditFraction: updating the credit to " +
          newCredit);
    }
    if (flowControl != null) {
      ((CreditBasedFlowControl) flowControl).updateCredit(newCredit);
    }
  }

  /**
   * Reset partitions and messages meta data. Also, reset the cached value of
   * superstep counter.
   */
  public void reset() {
    metaPartitionManager.resetPartitions();
    metaPartitionManager.resetMessages();
    superstep = service.getSuperstep();
    resetDone = true;
  }

  /**
   * @return cached value of the superstep counter
   */
  public long getSuperstep() {
    return superstep;
  }

  /**
   * Notify the out-of-core engine that a GC has just been completed
   *
   * @param info GC information
   */
  public void gcCompleted(GarbageCollectionNotificationInfo info) {
    oracle.gcCompleted(info);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    superstepMetrics.getGauge(GRAPH_PERCENTAGE_IN_MEMORY, new Gauge<Double>() {
      @Override
      public Double value() {
        return metaPartitionManager.getGraphFractionInMemory() * 100;
      }
    });
  }

  public FlowControl getFlowControl() {
    return flowControl;
  }

  public void setFlowControl(FlowControl flowControl) {
    this.flowControl = flowControl;
  }

  public OutOfCoreDataAccessor getDataAccessor() {
    return dataAccessor;
  }

  public NetworkMetrics getNetworkMetrics() {
    return networkMetrics;
  }
}
