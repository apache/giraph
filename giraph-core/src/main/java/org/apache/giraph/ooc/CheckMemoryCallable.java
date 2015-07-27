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

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Adaptive out-of-core mechanism brain. This class provides one thread per
 * worker that periodically checks the free memory on the worker and compares it
 * with total amount of memory given to that worker to run the job. The period
 * at which the thread checks for the memory is specified by the user. Also,
 * user can specify the fraction of memory where anytime free memory is less
 * than that fraction of total memory, actions would be taken to free up space
 * in memory (this fraction is called LOW_FREE_MEMORY_FRACTION). Also, user can
 * specify another fraction of available memory where memory pressure is fair
 * and some of the data on disk (if there is any) can be brought back to memory
 * again (this fraction is called FAIR_FREE_MEMORY_FRACTION).
 *
 * In the adaptive out-of-core mechanism, if amount of free memory becomes less
 * than LOW_FREE_MEMORY_FRACTION, some data are being considered as potentials
 * to transfer to disk. These data can be in the following categories:
 *   1) Vertex buffers read in INPUT_SUPERSTEP. These are vertex input splits
 *      read for a partition that is out-of-core and PartitionStore holds these
 *      vertex buffers in in-memory buffers (and postpone their merge with the
 *      actual partition until the partition is loaded back in memory).
 *   2) Edge buffers read in INPUT_SUPERSTEP. These are similar buffers to
 *      vertex buffers, but they keep edge data in INPUT_SUPERSTEP.
 *   3) Partitions.
 *
 * This brain prefers the first two categories in INPUT_SUPERSTEP as long as
 * size of buffers are large enough that it is worth writing them to disk. In
 * case where brain decides on spilling partitions to disk, the brain decides
 * only on the "number of partitions" to spill to disk. It is "out-of-core
 * processor threads" responsibility to find that many partitions to spill to
 * disk. The number of partitions to spill is a fraction of number of partitions
 * currently in memory. It is recommended that this fraction be equal to
 * subtraction of LOW_FREE_MEMORY_FRACTION from FAIR_FREE_MEMORY_FRACTION. Here
 * is an example to clarify on this recommendation. Assume
 * LOW_FREE_MEMORY_FRACTION is 5% and FAIR_FREE_MEMORY_FRACTION is 15%. Also
 * assume that the partitions are similar in their memory footprint (which is a
 * valid assumption for most of the partitioning techniques). If free memory is
 * a bit less than 5% of total available memory, if we offload 10%
 * (15% - 5% = 10%), then the amount of free memory will increase to a bit less
 * than 15% of total available memory.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class CheckMemoryCallable<I extends WritableComparable,
    V extends Writable, E extends Writable> implements Callable<Void> {
  /**
   * Lowest free memory fraction to start doing necessary actions to go
   * out-of-core.
   */
  public static final FloatConfOption LOW_FREE_MEMORY_FRACTION =
      new FloatConfOption("giraph.lowFreeMemoryFraction", 0.1f,
          "If free memory fraction goes below this value, GC is called " +
              "manually and necessary actions are taken if we have to go " +
              "out-of-core");
  /**
   * Expected memory fraction to achieve after detecting that the job is running
   * low in memory. Basically, this memory fraction is the target to achieve
   * once we decide to offload data on disk.
   */
  public static final FloatConfOption MID_FREE_MEMORY_FRACTION =
      new FloatConfOption("giraph.midFreeMemoryFraction", 0.15f,
          "Once out-of-core mechanism decides to offload data on disk, it " +
              "offloads data on disk until free memory fraction reaches this " +
              "fraction.");
  /**
   * Memory fraction at which the job gets the best performance considering the
   * choice of GC strategy. It means, if the amount of free memory is more than
   * this fraction we will not see severe amount of GC calls.
   */
  public static final FloatConfOption FAIR_FREE_MEMORY_FRACTION =
      new FloatConfOption("giraph.fairFreeMemoryFraction", 0.3f,
          "The fraction of free memory at which the job shows the best GC " +
              "performance. This fraction might be dependent on GC strategy " +
              "used in running the job, but generally 0.3 is a reasonable " +
              "fraction for most strategies.");
  /**
   * Memory fraction at which the job has enough space so we can back off from
   * the last out-of-core decision, i.e. lazily bringing the last bunch of data
   * spilled to disk.
   */
  public static final FloatConfOption HIGH_FREE_MEMORY_FRACTION =
      new FloatConfOption("giraph.highFreeMemoryFraction", 0.4f,
          "Once free memory reaches at this fraction, last out-of-core " +
              "decision is lazily rolled back, i.e. we back off from " +
              "out-of-core.");
  /** Time interval at which checking memory is done periodically. */
  public static final IntConfOption CHECK_MEMORY_INTERVAL =
      new IntConfOption("giraph.checkMemoryInterval", 5000,
          "Time interval (in milliseconds) at which checking memory is done" +
              " to decide if there should be any out-of-core action.");
  /** Coefficient by which the number of partitions in memory changes. */
  public static final FloatConfOption OOC_GRAPH_MODIFICATION_COEFFICIENT =
      new FloatConfOption("giraph.graphPartitionModificationCoefficient", 0.3f,
          "If we decide to go out-of-core or back-off from out-of-core, this " +
              "is the multiplier by which the number of in-memory partitions" +
              "will change.");

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CheckMemoryCallable.class);

  /** Worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;
  /** Partition store */
  private final DiskBackedPartitionStore<I, V, E> partitionStore;

  // ---- Cached Config Values ----
  /** Cached value of LOW_FREE_MEMORY_FRACTION */
  private float lowFreeMemoryFraction;
  /** Cached value for MID_FREE_MEMORY_FRACTION */
  private float midFreeMemoryFraction;
  /** Cached value of FAIR_FREE_MEMORY_FRACTION */
  private float fairFreeMemoryFraction;
  /** Cached value for HIGH_FREE_MEMORY_FRACTION */
  private float highFreeMemoryFraction;
  /** Cached value of CHECK_MEMORY_INTERVAL */
  private int checkInterval;
  /** Cached value for OOC_GRAPH_MODIFICATION_COEFFICIENT */
  private float modificationCoefficient;

  /** List of counts of number of partitions every time we shrink the store */
  private Stack<Integer> oocPartitionCounts;
  /** Memory estimator instance */
  private final MemoryEstimator memoryEstimator;
  /** Adaptive out-of-core engine */
  private final AdaptiveOutOfCoreEngine<I, V, E> oocEngine;

  /**
   * Constructor for check-memory thread.
   *
   * @param oocEngine out-of-core engine
   * @param conf job configuration
   * @param serviceWorker worker service
   */
  public CheckMemoryCallable(AdaptiveOutOfCoreEngine<I, V, E> oocEngine,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.oocEngine = oocEngine;
    this.serviceWorker = serviceWorker;
    this.partitionStore =
        (DiskBackedPartitionStore<I, V, E>) serviceWorker.getPartitionStore();

    this.oocPartitionCounts = new Stack<>();

    this.lowFreeMemoryFraction = LOW_FREE_MEMORY_FRACTION.get(conf);
    this.midFreeMemoryFraction = MID_FREE_MEMORY_FRACTION.get(conf);
    this.fairFreeMemoryFraction = FAIR_FREE_MEMORY_FRACTION.get(conf);
    this.highFreeMemoryFraction = HIGH_FREE_MEMORY_FRACTION.get(conf);
    this.checkInterval = CHECK_MEMORY_INTERVAL.get(conf);
    this.modificationCoefficient = OOC_GRAPH_MODIFICATION_COEFFICIENT.get(conf);

    memoryEstimator = ReflectionUtils
        .newInstance(GiraphConstants.OUT_OF_CORE_MEM_ESTIMATOR.get(conf));
  }

  /**
   * Checks whether the available free memory is enough for an efficient
   * execution. If memory is limited, offload partitions to disk.
   * Also, if available memory is more than a threshold, loads partitions from
   * disk (if there is any) to memory.
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_GC")
  public Void call() {
    if (LOG.isInfoEnabled()) {
      LOG.info("call: check-memory thread started.");
    }
    memoryEstimator.initialize(serviceWorker);
    CountDownLatch doneCompute = oocEngine.getDoneCompute();
    while (doneCompute.getCount() != 0) {
      double maxMemory = memoryEstimator.maxMemoryMB();
      double freeMemory = memoryEstimator.freeMemoryMB();
      boolean gcDone = false;
      if (freeMemory < lowFreeMemoryFraction * maxMemory) {
        // This is typically a bad scenario where previous GCs were not
        // successful to free up enough memory. If we keep staying in this
        // situation, usually, either the computation slows down dramatically,
        // or the computation throws OOM error. So, we do GC manually, and
        // make sure that out-of-core is the solution to get out of this
        // situation.
        if (LOG.isInfoEnabled()) {
          LOG.info("call: Memory is very limited now. Calling GC manually. " +
              String.format("freeMemory = %.2fMB", freeMemory));
        }
        long gcStartTime = System.currentTimeMillis();
        System.gc();
        gcDone = true;
        freeMemory = memoryEstimator.freeMemoryMB();
        if (LOG.isInfoEnabled()) {
          LOG.info("call: GC is done. " + String
              .format("GC time = %.2f sec, and freeMemory = %.2fMB",
                  (System.currentTimeMillis() - gcStartTime) / 1000.0,
                  freeMemory));
        }
      }

      // If we have enough memory, we roll back the latest shrink in number of
      // partition slots.
      // If we do not have enough memory, but we are not in a bad scenario
      // either, we gradually increase the number of partition slots in memory.
      // If we are low in free memory, we first push unnecessary data to disk
      // and then push some partitions to disk if necessary.
      int numInMemory = partitionStore.getNumPartitionInMemory();
      int maxInMemory = partitionStore.getNumPartitionSlots();
      int numInTotal = partitionStore.getNumPartitions();
      if (freeMemory > highFreeMemoryFraction * maxMemory) {
        if (numInMemory >= maxInMemory && !oocPartitionCounts.isEmpty()) {
          partitionStore.increasePartitionSlots(oocPartitionCounts.pop());
        }
      } else if (freeMemory > fairFreeMemoryFraction * maxMemory) {
        // Only gradually increase the number of partition slots if all slots
        // are already used, and we have things out-of-core
        if (!oocPartitionCounts.isEmpty() || maxInMemory < numInTotal) {
          if (numInMemory >= maxInMemory) {
            partitionStore.increasePartitionSlots(1);
            if (!oocPartitionCounts.isEmpty()) {
              int num = oocPartitionCounts.pop();
              if (num > 1) {
                oocPartitionCounts.push(num - 1);
              }
            }
          }
        }
      } else if (gcDone && freeMemory < midFreeMemoryFraction * maxMemory) {
        BlockingQueue<Integer> partitionsWithInputVertices =
            oocEngine.getPartitionsWithInputVertices();
        BlockingQueue<Integer> partitionsWithInputEdges =
            oocEngine.getPartitionsWithInputEdges();
        AtomicInteger numPartitionsToSpill =
            oocEngine.getNumPartitionsToSpill();

        while (freeMemory < midFreeMemoryFraction * maxMemory) {
          // Offload input vertex buffer of OOC partitions if we are in
          // INPUT_SUPERSTEP
          if (serviceWorker.getSuperstep() == BspService.INPUT_SUPERSTEP) {
            // List of pairs (partitionId, approximate memory footprint of
            // vertex buffers of that partition).
            PairList<Integer, Integer> pairs =
                partitionStore.getOocPartitionIdsWithPendingInputVertices();
            freeMemory -= createCommand(pairs, partitionsWithInputVertices);
          }

          // Offload edge store of OOC partitions if we are in INPUT_SUPERSTEP
          if (freeMemory < midFreeMemoryFraction * maxMemory &&
              serviceWorker.getSuperstep() == BspService.INPUT_SUPERSTEP) {
            PairList<Integer, Integer> pairs =
                partitionStore.getOocPartitionIdsWithPendingInputEdges();
            freeMemory -= createCommand(pairs, partitionsWithInputEdges);
          }

          // Offload partitions if we are still low in free memory
          if (freeMemory < midFreeMemoryFraction * maxMemory) {
            numPartitionsToSpill
                .set(getNextOocPartitionCount(freeMemory, maxMemory));
          }

          if (!partitionsWithInputVertices.isEmpty() ||
              !partitionsWithInputEdges.isEmpty() ||
              numPartitionsToSpill.get() != 0) {
            if (LOG.isInfoEnabled()) {
              LOG.info("call: signal out-of-core processor threads to start " +
                  "offloading. These threads will spill vertex buffer of " +
                  partitionsWithInputVertices.size() + " partitions, edge " +
                  "buffers of " + partitionsWithInputEdges.size() +
                  " partitions, and " + numPartitionsToSpill.get() + " whole " +
                  "partition");
            }
            // Opening the gate for OOC processing threads to start spilling
            // data on disk
            try {
              oocEngine.waitOnGate();
            } catch (InterruptedException e) {
              throw new IllegalStateException("call: Caught " +
                  "InterruptedException while opening the gate for OOC " +
                  "processing threads");
            } catch (BrokenBarrierException e) {
              throw new IllegalStateException("call: Caught " +
                  "BrokenBarrierException while opening the gate for OOC " +
                  "processing threads");
            }
            oocEngine.resetGate();

            if (LOG.isInfoEnabled()) {
              LOG.info("call: waiting on OOC processors to finish offloading " +
                  "data to disk");
            }
            // Wait until all OOC processing threads are done swapping data to
            // disk
            try {
              oocEngine.waitOnOocSignal();
            } catch (InterruptedException e) {
              throw new IllegalStateException("call: Caught " +
                  "InterruptedException. Looks like memory check thread is " +
                  "interrupted while waiting on OOC processing threads.");
            } catch (BrokenBarrierException e) {
              throw new IllegalStateException("call: Caught " +
                  "BrokenBarrierException. Looks like some OOC processing " +
                  "threads  broke while writing data on disk.");
            }
            oocEngine.resetOocSignal();
          }

          gcDone = false;
          long gcStartTime = 0;
          if (freeMemory < midFreeMemoryFraction * maxMemory) {
            // Calling GC manually to actually free up the memory for data that
            // is offloaded to disk
            if (LOG.isInfoEnabled()) {
              LOG.info("call: calling GC manually to free up space for " +
                  "recently offloaded data.");
            }
            gcStartTime = System.currentTimeMillis();
            System.gc();
            gcDone = true;
          }
          freeMemory = memoryEstimator.freeMemoryMB();
          if (LOG.isInfoEnabled()) {
            LOG.info("call: " +
                (gcDone ?
                    ("GC is done. " + String.format("GC time = %.2f sec.",
                        (System.currentTimeMillis() - gcStartTime) / 1000.0)) :
                    "") +
                "Finished offloading data to disk. " +
                String.format("freeMemory = %.2fMB", freeMemory));
          }
        }
      }

      // Either wait for the computation to be done, or the time interval passes
      try {
        doneCompute.await(checkInterval, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException("call: Caught InterruptedException " +
            "while waiting for computation to be done and/or " + checkInterval +
            "milliseconds passes.");
      }
    }

    // Setting 'done' before the gate here and checking 'done' in OOC processing
    // threads after the gate, guarantees that OOC processing threads see the
    // new value of done and terminate gracefully.
    oocEngine.setDone();
    try {
      oocEngine.waitOnGate();
    } catch (InterruptedException e) {
      throw new IllegalStateException("call: Caught InterruptedException " +
          "while waiting for the last time on gate in the current superstep");
    } catch (BrokenBarrierException e) {
      throw new IllegalStateException("call: Caught BrokenBarrierException " +
          "while waiting for the last time on gate in the current superstep");
    }
    return null;
  }

  /**
   * Returns the number of partitions that should go out-of-core at this point.
   *
   * @return number of partitions that should go out-of-core
   * @param freeMemory amount of free memory (in MB)
   * @param maxMemory amount of max memory (in MB)
   */
  private int getNextOocPartitionCount(double freeMemory, double maxMemory) {
    int numSlots = partitionStore.getNumPartitionSlots();
    if (numSlots == Integer.MAX_VALUE) {
      numSlots = partitionStore.getNumPartitions();
      partitionStore.setNumPartitionSlots(numSlots);
    }

    double freeFraction = freeMemory / maxMemory;
    double multiplier = Math.min(
        // User-specified favorable size to spill to disk
        modificationCoefficient,
        // Approximate fraction of current data to spill in order to reach the
        // fair fraction of free memory
        (fairFreeMemoryFraction - freeFraction) / (1 - freeFraction));
    int count = Math.max((int) (numSlots * multiplier), 1);
    if (count >= numSlots) {
      LOG.warn("getNextOocPartitionCount: Memory capacity is " +
          numSlots + " partitions, and OOC mechanism is " +
          "trying to put " + count + " partitions to disk. This is not " +
          "possible");
      // We should have at least one partition in memory
      count = numSlots - 1;
      if (count == 0) {
        LOG.warn("It seems that size of one partition is too large for the " +
            "available memory.  Try to run the job with more partitions!");
      }
    }
    if (count != 0) {
      oocPartitionCounts.push(count);
    }
    return count;
  }

  /**
   * Generates the command for a particular type of data we want to offload to
   * disk.
   *
   * @param pairs list of pair(partitionId, approximate foot-print that is going
   *              of be reduced by offloading the particular data of a
   *              partition)
   * @param commands list of partitionIds for which we want to execute the
   *                 command
   * @return approximate amount of memory (in MB) that is going to be freed up
   *         after executing the generated commands
   */
  private double createCommand(PairList<Integer, Integer> pairs,
      BlockingQueue<Integer> commands) {
    double usedMemory = 0;
    if (pairs.getSize() != 0) {
      PairList<Integer, Integer>.Iterator iterator = pairs.getIterator();
      // Generating commands for out-of-core processor threads to
      // offload data as long as command queue has space.
      while (iterator.hasNext() &&
          commands.remainingCapacity() > 0) {
        iterator.next();
        commands.add(iterator.getCurrentFirst());
        // Having an approximation on the memory foot-print of data to offload
        // helps us to know how much memory is going to become available by
        // offloading the data without using internal functions to estimate
        // free memory again.
        usedMemory += iterator.getCurrentSecond() / 1024.0 / 1024.0;
      }
    }
    return usedMemory;
  }
}
