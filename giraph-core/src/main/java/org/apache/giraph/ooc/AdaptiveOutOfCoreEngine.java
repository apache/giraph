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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.LogStacktraceCallable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Adaptive out-of-core mechanism. This mechanism spawns two types of threads:
 *   1) check-memory thread, which periodically monitors the amount of available
 *      memory and decides whether data should go on disk. This threads is
 *      basically the brain behind the out-of-core mechanism, commands
 *      "out-of-core processor threads" (type 2 thread below) to move
 *      appropriate data to disk,
 *   2) out-of-core processor threads. This is a team of threads responsible for
 *      offloading appropriate data to disk. "check-memory thread" decides on
 *      which data should go to disk, and "out-of-core processor threads" do the
 *      offloading.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge data
 */
public class AdaptiveOutOfCoreEngine<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    OutOfCoreEngine<I, V, E> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(AdaptiveOutOfCoreEngine.class);

  // ---- Synchronization Variables ----
  /** Barrier to coordinate check-memory and OOC-processing threads */
  private final CyclicBarrier gate;
  /**
   * Signal to determine whether OOC processing threads are done processing OOC
   * requests
   */
  private final CyclicBarrier doneOocSignal;
  /** Signal to determine whether the computation is terminated */
  private final CountDownLatch doneCompute;
  /** Finisher signal to OOC processing threads */
  private volatile boolean done;

  // ---- OOC Commands ----
  /**
   * List of partitions that are on disk, and their loaded *vertices* during
   * INPUT_SUPERSTEP are ready to flush to disk
   */
  private final BlockingQueue<Integer> partitionsWithInputVertices;
  /**
   * List of partitions that are on disk, and their loaded *edges* during
   * INPUT_SUPERSTEP are ready to flush to disk
   */
  private final BlockingQueue<Integer> partitionsWithInputEdges;
  /** Number of partitions to be written to the disk */
  private final AtomicInteger numPartitionsToSpill;

  /** Executor service for check memory thread */
  private ExecutorService checkMemoryExecutor;
  /** Executor service for out-of-core processor threads */
  private ExecutorService outOfCoreProcessorExecutor;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  /** Cached value for number of out-of-core threads specified by user */
  private int numOocThreads;

  /** Result of check-memory thread (to be checked for graceful termination) */
  private Future<Void> checkMemoryResult;
  /**
   * Results of out-of-core processor threads (to be checked for graceful
   * termination)
   */
  private List<Future<Void>> oocProcessorResults;

  /**
   * Creates an instance of adaptive mechanism
   * @param conf Configuration
   * @param serviceWorker Worker service
   */
  public AdaptiveOutOfCoreEngine(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.serviceWorker = serviceWorker;

    this.numOocThreads = conf.getNumOocThreads();
    this.gate = new CyclicBarrier(numOocThreads + 1);
    this.doneOocSignal = new CyclicBarrier(numOocThreads + 1);
    this.doneCompute = new CountDownLatch(1);
    this.done = false;
    this.partitionsWithInputVertices = new ArrayBlockingQueue<Integer>(100);
    this.partitionsWithInputEdges = new ArrayBlockingQueue<Integer>(100);
    this.numPartitionsToSpill = new AtomicInteger(0);
  }

  @Override
  public void initialize() {
    if (LOG.isInfoEnabled()) {
      LOG.info("initialize: initializing out-of-core engine");
    }
    CallableFactory<Void> checkMemoryCallableFactory =
      new CallableFactory<Void>() {
        @Override
        public Callable<Void> newCallable(int callableId) {
          return new CheckMemoryCallable<I, V, E>(
              AdaptiveOutOfCoreEngine.this, conf, serviceWorker);
        }
      };
    checkMemoryExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("check-memory").build());
    checkMemoryResult = checkMemoryExecutor.submit(new LogStacktraceCallable<>(
        checkMemoryCallableFactory.newCallable(0)));

    CallableFactory<Void> outOfCoreProcessorCallableFactory =
      new CallableFactory<Void>() {
        @Override
        public Callable<Void> newCallable(int callableId) {
          return new OutOfCoreProcessorCallable<I, V, E>(
              AdaptiveOutOfCoreEngine.this, serviceWorker);
        }
      };
    outOfCoreProcessorExecutor = Executors
        .newFixedThreadPool(numOocThreads,
            new ThreadFactoryBuilder().setNameFormat("ooc-%d").build());
    oocProcessorResults = Lists.newArrayListWithCapacity(numOocThreads);
    for (int i = 0; i < numOocThreads; ++i) {
      Future<Void> future = outOfCoreProcessorExecutor.submit(
          new LogStacktraceCallable<>(
              outOfCoreProcessorCallableFactory.newCallable(i)));
      oocProcessorResults.add(future);
    }
  }

  @Override
  public void shutdown() {
    doneCompute.countDown();
    checkMemoryExecutor.shutdown();
    if (checkMemoryResult.isCancelled()) {
      throw new IllegalStateException(
          "shutdown: memory check thread did not " + "terminate gracefully!");
    }
    outOfCoreProcessorExecutor.shutdown();
    for (int i = 0; i < numOocThreads; ++i) {
      if (oocProcessorResults.get(i).isCancelled()) {
        throw new IllegalStateException("shutdown: out-of-core processor " +
            "thread " + i + " did not terminate gracefully.");
      }
    }
  }

  /**
   * @return the latch that signals whether the whole computation is done
   */
  public CountDownLatch getDoneCompute() {
    return doneCompute;
  }

  /**
   * @return whether the computation is done
   */
  public boolean isDone() {
    return done;
  }

  /**
   * @return list of partitions that have large enough buffers of vertices read
   *         in INPUT_SUPERSTEP.
   */
  public BlockingQueue<Integer> getPartitionsWithInputVertices() {
    return partitionsWithInputVertices;
  }

  /**
   * @return list of partitions that have large enough buffers of edges read
   *         in INPUT_SUPERSTEP.
   */
  public BlockingQueue<Integer> getPartitionsWithInputEdges() {
    return partitionsWithInputEdges;
  }

  /**
   * @return number of partitions to spill to disk
   */
  public AtomicInteger getNumPartitionsToSpill() {
    return numPartitionsToSpill;
  }

  /**
   * Wait on gate with which OOC processor threads are notified to execute
   * commands provided by brain (memory-check thread).
   *
   * @throws BrokenBarrierException
   * @throws InterruptedException
   */
  public void waitOnGate() throws BrokenBarrierException, InterruptedException {
    gate.await();
  }

  /**
   * Reset the gate for reuse.
   */
  public void resetGate() {
    gate.reset();
  }

  /**
   * Wait on signal from all OOC processor threads that the offloading of data
   * is complete.
   *
   * @throws BrokenBarrierException
   * @throws InterruptedException
   */
  public void waitOnOocSignal()
      throws BrokenBarrierException, InterruptedException {
    doneOocSignal.await();
  }

  /**
   * Reset the completion signal of OOC processor threads for reuse.
   */
  public void resetOocSignal() {
    doneOocSignal.reset();
  }

  /**
   * Set the computation as done (i.e. setting the state that determines the
   * whole computation is done).
   */
  public void setDone() {
    done = true;
  }
}
