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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.giraph.comm.NetworkMetrics;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.AbstractEdgeStore;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.command.WaitIOCommand;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.giraph.worker.EdgeInputSplitsCallable;
import org.apache.giraph.worker.VertexInputSplitsCallable;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of {@link OutOfCoreOracle} that uses a linear regression model
 * to estimate actual memory usage based on the current state of computation.
 * The model takes into consideration 5 parameters:
 *
 * y = c0 + c1*x1 + c2*x2 + c3*x3 + c4*x4 + c5*x5
 *
 * y: memory usage
 * x1: edges loaded
 * x2: vertices loaded
 * x3: vertices processed
 * x4: bytes received due to messages
 * x5: bytes loaded/stored from/to disk due to OOC.
 *
 */
public class MemoryEstimatorOracle implements OutOfCoreOracle {
  /** Memory check interval in msec */
  public static final LongConfOption CHECK_MEMORY_INTERVAL =
    new LongConfOption("giraph.garbageEstimator.checkMemoryInterval", 1000,
        "The interval where memory checker thread wakes up and " +
            "monitors memory footprint (in milliseconds)");
  /**
   * If mem-usage is above this threshold and no Full GC has been called,
   * we call it manually
   */
  public static final FloatConfOption MANUAL_GC_MEMORY_PRESSURE =
    new FloatConfOption("giraph.garbageEstimator.manualGCPressure", 0.95f,
        "The threshold above which GC is called manually if Full GC has not " +
            "happened in a while");
  /** Used to detect a high memory pressure situation */
  public static final FloatConfOption GC_MINIMUM_RECLAIM_FRACTION =
    new FloatConfOption("giraph.garbageEstimator.gcReclaimFraction", 0.05f,
        "Minimum percentage of memory we expect to be reclaimed after a Full " +
            "GC. If less than this amount is reclaimed, it is sage to say " +
            "we are in a high memory situation and the estimation mechanism " +
            "has not recognized it yet!");
  /** If mem-usage is above this threshold, active threads are set to 0 */
  public static final FloatConfOption AM_HIGH_THRESHOLD =
    new FloatConfOption("giraph.amHighThreshold", 0.95f,
        "If mem-usage is above this threshold, all active threads " +
            "(compute/input) are paused.");
  /** If mem-usage is below this threshold, active threads are set to max */
  public static final FloatConfOption AM_LOW_THRESHOLD =
    new FloatConfOption("giraph.amLowThreshold", 0.90f,
        "If mem-usage is below this threshold, all active threads " +
            "(compute/input) are running.");
  /** If mem-usage is above this threshold, credit is set to 0 */
  public static final FloatConfOption CREDIT_HIGH_THRESHOLD =
    new FloatConfOption("giraph.creditHighThreshold", 0.95f,
        "If mem-usage is above this threshold, credit is set to 0");
  /** If mem-usage is below this threshold, credit is set to max */
  public static final FloatConfOption CREDIT_LOW_THRESHOLD =
    new FloatConfOption("giraph.creditLowThreshold", 0.90f,
        "If mem-usage is below this threshold, credit is set to max");
  /** OOC starts if mem-usage is above this threshold */
  public static final FloatConfOption OOC_THRESHOLD =
    new FloatConfOption("giraph.oocThreshold", 0.90f,
        "If mem-usage is above this threshold, out of core threads starts " +
            "writing data to disk");

  /** Logger */
  private static final Logger LOG =
    Logger.getLogger(MemoryEstimatorOracle.class);

  /** Cached value for {@link #MANUAL_GC_MEMORY_PRESSURE} */
  private final float manualGCMemoryPressure;
  /** Cached value for {@link #GC_MINIMUM_RECLAIM_FRACTION} */
  private final float gcReclaimFraction;
  /** Cached value for {@link #AM_HIGH_THRESHOLD} */
  private final float amHighThreshold;
  /** Cached value for {@link #AM_LOW_THRESHOLD} */
  private final float amLowThreshold;
  /** Cached value for {@link #CREDIT_HIGH_THRESHOLD} */
  private final float creditHighThreshold;
  /** Cached value for {@link #CREDIT_LOW_THRESHOLD} */
  private final float creditLowThreshold;
  /** Cached value for {@link #OOC_THRESHOLD} */
  private final float oocThreshold;

  /** Reference to running OOC engine */
  private final OutOfCoreEngine oocEngine;
  /** Memory estimator instance */
  private final MemoryEstimator memoryEstimator;
  /** Keeps track of the number of bytes stored/loaded by OOC */
  private final AtomicLong oocBytesInjected = new AtomicLong(0);
  /** How many bytes to offload */
  private final AtomicLong numBytesToOffload = new AtomicLong(0);
  /** Current state of the OOC */
  private volatile State state = State.STABLE;
  /** Timestamp of the last major GC */
  private volatile long lastMajorGCTime = 0;

  /**
   * Different states the OOC can be in.
   */
  private enum State {
    /** No offloading */
    STABLE,
    /** Current offloading */
    OFFLOADING,
  }

  /**
   * Constructor.
   * @param conf Configuration
   * @param oocEngine OOC engine.:w
   *
   */
  public MemoryEstimatorOracle(ImmutableClassesGiraphConfiguration conf,
                               final OutOfCoreEngine oocEngine) {
    this.oocEngine = oocEngine;
    this.memoryEstimator = new MemoryEstimator(this.oocBytesInjected,
      oocEngine.getNetworkMetrics());

    this.manualGCMemoryPressure = MANUAL_GC_MEMORY_PRESSURE.get(conf);
    this.gcReclaimFraction = GC_MINIMUM_RECLAIM_FRACTION.get(conf);
    this.amHighThreshold = AM_HIGH_THRESHOLD.get(conf);
    this.amLowThreshold = AM_LOW_THRESHOLD.get(conf);
    this.creditHighThreshold = CREDIT_HIGH_THRESHOLD.get(conf);
    this.creditLowThreshold = CREDIT_LOW_THRESHOLD.get(conf);
    this.oocThreshold = OOC_THRESHOLD.get(conf);

    final long checkMemoryInterval = CHECK_MEMORY_INTERVAL.get(conf);

    ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          long oldGenUsageEstimate = memoryEstimator.getUsageEstimate();
          MemoryUsage usage = getOldGenUsed();
          if (oldGenUsageEstimate > 0) {
            updateRates(oldGenUsageEstimate, usage.getMax());
          } else {
            long time = System.currentTimeMillis();
            if (time - lastMajorGCTime >= 10000) {
              double used = (double) usage.getUsed() / usage.getMax();
              if (used > manualGCMemoryPressure) {
                if (LOG.isInfoEnabled()) {
                  LOG.info(
                    "High memory pressure with no full GC from the JVM. " +
                      "Calling GC manually. Used fraction of old-gen is " +
                      String.format("%.2f", used) + ".");
                }
                System.gc();
                time = System.currentTimeMillis() - time;
                usage = getOldGenUsed();
                used = (double) usage.getUsed() / usage.getMax();
                if (LOG.isInfoEnabled()) {
                  LOG.info("Manual GC done. It took " +
                    String.format("%.2f", time / 1000.0) +
                    " seconds. Used fraction of old-gen is " +
                    String.format("%.2f", used) + ".");
                }
              }
            }
          }
          try {
            Thread.sleep(checkMemoryInterval);
          } catch (InterruptedException e) {
            LOG.warn("run: exception occurred!", e);
            return;
          }
        }
      }
    }, "ooc-memory-checker", oocEngine.getServiceWorker().getGraphTaskManager()
        .createUncaughtExceptionHandler());
  }

  /**
   * Resets all the counters used in the memory estimation. This is called at
   * the beginning of a new superstep.
   * <p>
   * The number of vertices to compute in the next superstep gets reset in
   * {@link org.apache.giraph.graph.GraphTaskManager#processGraphPartitions}
   * right before
   * {@link org.apache.giraph.partition.PartitionStore#startIteration()} gets
   * called.
   */
  @Override
  public void startIteration() {
    AbstractEdgeStore.PROGRESS_COUNTER.reset();
    oocBytesInjected.set(0);
    memoryEstimator.clear();
    memoryEstimator.setCurrentSuperstep(oocEngine.getSuperstep());
    oocEngine.updateRequestsCreditFraction(1);
    oocEngine.updateActiveThreadsFraction(1);
  }


  @Override
  public IOAction[] getNextIOActions() {
    if (state == State.OFFLOADING) {
      return new IOAction[]{
        IOAction.STORE_MESSAGES_AND_BUFFERS, IOAction.STORE_PARTITION};
    }
    long oldGenUsage = memoryEstimator.getUsageEstimate();
    MemoryUsage usage = getOldGenUsed();
    if (oldGenUsage > 0) {
      double usageEstimate = (double) oldGenUsage / usage.getMax();
      if (usageEstimate > oocThreshold) {
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PARTITION};
      } else {
        return new IOAction[]{IOAction.LOAD_PARTITION};
      }
    } else {
      return new IOAction[]{IOAction.LOAD_PARTITION};
    }
  }

  @Override
  public boolean approve(IOCommand command) {
    return true;
  }

  @Override
  public void commandCompleted(IOCommand command) {
    if (command instanceof LoadPartitionIOCommand) {
      oocBytesInjected.getAndAdd(command.bytesTransferred());
      if (state == State.OFFLOADING) {
        numBytesToOffload.getAndAdd(command.bytesTransferred());
      }
    } else if (!(command instanceof WaitIOCommand)) {
      oocBytesInjected.getAndAdd(0 - command.bytesTransferred());
      if (state == State.OFFLOADING) {
        numBytesToOffload.getAndAdd(0 - command.bytesTransferred());
      }
    }

    if (state == State.OFFLOADING && numBytesToOffload.get() <= 0) {
      numBytesToOffload.set(0);
      state = State.STABLE;
      updateRates(-1, 1);
    }
  }

  /**
   * When a new GC has completed, we can get an accurate measurement of the
   * memory usage. We use this to update the linear regression model.
   *
   * @param gcInfo GC information
   */
  @Override
  public synchronized void gcCompleted(
    GarbageCollectionNotificationInfo gcInfo) {
    String action = gcInfo.getGcAction().toLowerCase();
    String cause = gcInfo.getGcCause().toLowerCase();
    if (action.contains("major") &&
      (cause.contains("ergo") || cause.contains("system"))) {
      lastMajorGCTime = System.currentTimeMillis();
      MemoryUsage before = null;
      MemoryUsage after = null;

      for (Map.Entry<String, MemoryUsage> entry :
        gcInfo.getGcInfo().getMemoryUsageBeforeGc().entrySet()) {
        String poolName = entry.getKey();
        if (poolName.toLowerCase().contains("old")) {
          before = entry.getValue();
          after = gcInfo.getGcInfo().getMemoryUsageAfterGc().get(poolName);
          break;
        }
      }
      if (after == null) {
        throw new IllegalStateException("Missing Memory Usage After GC info");
      }
      if (before == null) {
        throw new IllegalStateException("Missing Memory Usage Before GC info");
      }

      // Compare the estimation with the actual value
      long usedMemoryEstimate = memoryEstimator.getUsageEstimate();
      long usedMemoryReal = after.getUsed();
      if (usedMemoryEstimate >= 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("gcCompleted: estimate=" + usedMemoryEstimate + " real=" +
            usedMemoryReal + " error=" +
            ((double) Math.abs(usedMemoryEstimate - usedMemoryReal) /
              usedMemoryReal * 100));
        }
      }

      // Number of edges loaded so far (if in input superstep)
      long edgesLoaded = oocEngine.getSuperstep() >= 0 ? 0 :
        EdgeInputSplitsCallable.getTotalEdgesLoadedMeter().count();
      // Number of vertices loaded so far (if in input superstep)
      long verticesLoaded = oocEngine.getSuperstep() >= 0 ? 0 :
        VertexInputSplitsCallable.getTotalVerticesLoadedMeter().count();
      // Number of vertices computed (if either in compute or store phase)
      long verticesComputed = WorkerProgress.get().getVerticesComputed() +
        WorkerProgress.get().getVerticesStored() +
        AbstractEdgeStore.PROGRESS_COUNTER.getProgress();
      // Number of bytes received
      long receivedBytes =
        oocEngine.getNetworkMetrics().getBytesReceivedPerSuperstep();
      // Number of OOC bytes
      long oocBytes = oocBytesInjected.get();

      memoryEstimator.addRecord(getOldGenUsed().getUsed(), edgesLoaded,
        verticesLoaded, verticesComputed, receivedBytes, oocBytes);

      long garbage = before.getUsed() - after.getUsed();
      long maxMem = after.getMax();
      long memUsed = after.getUsed();
      boolean isTight = (maxMem - memUsed) < 2 * gcReclaimFraction * maxMem &&
        garbage < gcReclaimFraction * maxMem;
      boolean predictionExist = memoryEstimator.getUsageEstimate() > 0;
      if (isTight && !predictionExist) {
        if (LOG.isInfoEnabled()) {
          LOG.info("gcCompleted: garbage=" + garbage + " memUsed=" +
            memUsed + " maxMem=" + maxMem);
        }
        numBytesToOffload.set((long) (2 * gcReclaimFraction * maxMem) -
          (maxMem - memUsed));
        if (LOG.isInfoEnabled()) {
          LOG.info("gcCompleted: tight memory usage. Starting to offload " +
            "until " + numBytesToOffload.get() + " bytes are offloaded");
        }
        state = State.OFFLOADING;
        updateRates(1, 1);
      }
    }
  }

  /**
   * Given an estimate for the current memory usage and the maximum available
   * memory, it updates the active threads and flow control credit in the
   * OOC engine.
   *
   * @param usageEstimateMem Estimate of memory usage.
   * @param maxMemory Maximum memory.
   */
  private void updateRates(long usageEstimateMem, long maxMemory) {
    double usageEstimate = (double) usageEstimateMem / maxMemory;
    if (usageEstimate > 0) {
      if (usageEstimate >= amHighThreshold) {
        oocEngine.updateActiveThreadsFraction(0);
      } else if (usageEstimate < amLowThreshold) {
        oocEngine.updateActiveThreadsFraction(1);
      } else {
        oocEngine.updateActiveThreadsFraction(1 -
          (usageEstimate - amLowThreshold) /
            (amHighThreshold - amLowThreshold));
      }

      if (usageEstimate >= creditHighThreshold) {
        oocEngine.updateRequestsCreditFraction(0);
      } else if (usageEstimate < creditLowThreshold) {
        oocEngine.updateRequestsCreditFraction(1);
      } else {
        oocEngine.updateRequestsCreditFraction(1 -
          (usageEstimate - creditLowThreshold) /
            (creditHighThreshold - creditLowThreshold));
      }
    } else {
      oocEngine.updateActiveThreadsFraction(1);
      oocEngine.updateRequestsCreditFraction(1);
    }
  }

  /**
   * Returns statistics about the old gen pool.
   * @return {@link MemoryUsage}.
   */
  private MemoryUsage getOldGenUsed() {
    List<MemoryPoolMXBean> memoryPoolList =
      ManagementFactory.getMemoryPoolMXBeans();
    for (MemoryPoolMXBean pool : memoryPoolList) {
      String normalName = pool.getName().toLowerCase();
      if (normalName.contains("old") || normalName.contains("tenured")) {
        return pool.getUsage();
      }
    }
    throw new IllegalStateException("Bad Memory Pool");
  }

  /**
   * Maintains statistics about the current state and progress of the
   * computation and produces estimates of memory usage using a technique
   * based on linear regression.
   *
   * Upon a GC events, it gets updated with the most recent statistics through
   * the {@link #addRecord} method.
   */
  private static class MemoryEstimator {
    /** Stores the (x1,x2,...,x5) arrays of data samples, one for each sample */
    private List<double[]> dataSamples = new ArrayList<>();
    /** Stores the y memory usage dataSamples, one for each sample */
    private DoubleArrayList memorySamples = new DoubleArrayList();
    /** Stores the coefficients computed by the linear regression model */
    private double[] coefficient = new double[6];
    /** Stores the column indices that can be used in the regression model */
    private List<Integer> validColumnIndices = new ArrayList<>();
    /** Potentially out-of-range coefficient values */
    private double[] extreme = new double[6];
    /** Indicates whether current coefficients can be used for estimation */
    private boolean isValid = false;
    /** Implementation of linear regression */
    private OLSMultipleLinearRegression mlr = new OLSMultipleLinearRegression();
    /** Used to synchronize access to the data samples */
    private Lock lock = new ReentrantLock();
    /** The estimation method depends on the current superstep. */
    private long currentSuperstep = -1;
    /** The estimation method depends on the bytes injected. */
    private final AtomicLong oocBytesInjected;
    /** Provides network statistics */
    private final NetworkMetrics networkMetrics;

    /**
     * Constructor
     * @param oocBytesInjected Reference to {@link AtomicLong} object
     *                         maintaining the number of OOC bytes stored.
     * @param networkMetrics Interface to get network stats.
     */
    public MemoryEstimator(AtomicLong oocBytesInjected,
                           NetworkMetrics networkMetrics) {
      this.oocBytesInjected = oocBytesInjected;
      this.networkMetrics = networkMetrics;
    }


    /**
     * Clear data structure (called from single threaded program).
     */
    public void clear() {
      dataSamples.clear();
      memorySamples.clear();
      isValid = false;
    }

    public void setCurrentSuperstep(long superstep) {
      this.currentSuperstep = superstep;
    }

    /**
     * Given the current state of computation (i.e. current edges loaded,
     * vertices computed etc) and the current model (i.e. the regression
     * coefficient), it returns a prediction about the memory usage in bytes.
     *
     * @return Memory estimate in bytes.
     */
    public long getUsageEstimate() {
      long usage = -1;
      lock.lock();
      try {
        if (isValid) {
          // Number of edges loaded so far (if in input superstep)
          long edgesLoaded = currentSuperstep >= 0 ? 0 :
            EdgeInputSplitsCallable.getTotalEdgesLoadedMeter().count();
          // Number of vertices loaded so far (if in input superstep)
          long verticesLoaded = currentSuperstep >= 0 ? 0 :
            VertexInputSplitsCallable.getTotalVerticesLoadedMeter().count();
          // Number of vertices computed (if either in compute or store phase)
          long verticesComputed = WorkerProgress.get().getVerticesComputed() +
            WorkerProgress.get().getVerticesStored() +
            AbstractEdgeStore.PROGRESS_COUNTER.getProgress();
          // Number of bytes received
          long receivedBytes = networkMetrics.getBytesReceivedPerSuperstep();
          // Number of OOC bytes
          long oocBytes = this.oocBytesInjected.get();

          usage = (long) (edgesLoaded * coefficient[0] +
            verticesLoaded * coefficient[1] +
            verticesComputed * coefficient[2] +
            receivedBytes * coefficient[3] +
            oocBytes * coefficient[4] +
            coefficient[5]);
        }
      } finally {
        lock.unlock();
      }
      return usage;
    }

    /**
     * Updates the linear regression model with a new data point.
     *
     * @param memUsed Current real value of memory usage.
     * @param edges Number of edges loaded.
     * @param vertices Number of vertices loaded.
     * @param verticesProcessed Number of vertices processed.
     * @param bytesReceived Number of bytes received.
     * @param oocBytesInjected Number of bytes stored/loaded due to OOC.
     */
    public void addRecord(long memUsed, long edges, long vertices,
                          long verticesProcessed,
                          long bytesReceived, long oocBytesInjected) {
      checkState(memUsed > 0, "Memory Usage cannot be negative");
      if (dataSamples.size() > 0) {
        double[] last = dataSamples.get(dataSamples.size() - 1);
        if (edges == last[0] && vertices == last[1] &&
          verticesProcessed == last[2] && bytesReceived == last[3] &&
          oocBytesInjected == last[4]) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "addRecord: avoiding to add the same entry as the last one!");
          }
          return;
        }
      }
      dataSamples.add(new double[] {edges, vertices, verticesProcessed,
        bytesReceived, oocBytesInjected});
      memorySamples.add((double) memUsed);

      // Weed out the columns that are all zero
      validColumnIndices.clear();
      for (int i = 0; i < 5; ++i) {
        boolean validIndex = false;
        // Check if there is a non-zero entry in the column
        for (double[] value : dataSamples) {
          if (value[i] != 0) {
            validIndex = true;
            break;
          }
        }
        if (validIndex) {
          // check if all entries are not equal to each other
          double firstValue = -1;
          boolean allEqual = true;
          for (double[] value : dataSamples) {
            if (firstValue == -1) {
              firstValue = value[i];
            } else {
              if (Math.abs((value[i] - firstValue) / firstValue) > 0.01) {
                allEqual = false;
                break;
              }
            }
          }
          validIndex = !allEqual;
          if (validIndex) {
            // Check if the column has linear dependency with another column
            for (int col = i + 1; col < 5; ++col) {
              if (isLinearDependence(dataSamples, i, col)) {
                validIndex = false;
                break;
              }
            }
          }
        }

        if (validIndex) {
          validColumnIndices.add(i);
        }
      }

      // If we filtered out columns in the previous step, we are going to run
      // the regression without those columns.

      // Create the coefficient table
      boolean setIsValid = false;
      lock.lock();
      try {
        if (validColumnIndices.size() >= 1 &&
          dataSamples.size() >= validColumnIndices.size() + 1) {

          double[][] xValues = new double[dataSamples.size()][];
          fillXMatrix(dataSamples, validColumnIndices, xValues);
          double[] yValues =
              memorySamples.toDoubleArray(new double[memorySamples.size()]);
          mlr.newSampleData(yValues, xValues);
          boolean isRegressionValid =
            calculateRegression(coefficient, validColumnIndices, mlr);

          if (!isRegressionValid) { // invalid regression result
            return; // The finally-block at the end will release any locks.
          }

          // After the computation of the regression, some coefficients may have
          // values outside the valid value range. In this case, we set the
          // coefficient to the minimum or maximum value allowed, and re-run the
          // regression.
          // We only care about coefficients of two of the variables:
          // bytes received due to messages (receivedBytes -- index 3 in
          // `coefficient` array) and bytes transferred due to OOC (oocBytes --
          // index 4 in `coefficient` array).
          //
          // receivedByte's coefficient cannot be negative, meaning that it does
          // not make sense that memory footprint decreases because of receipt
          // of messages. We either have message combiner or we do not have
          // combiner. If message combiner is used, the memory footprint
          // will not change much due to messages leading to the coefficient for
          // oocBytes to be close to 0. If message combiner is not used, the
          // memory only increase with messages, and the coefficient should be
          // positive. In this case, a message is usually deserialized and then
          // written to the message store. We assume that the process of
          // deserializing the message and putting it into the message store
          // will not result in more than twice the size of the serialized form
          // of the message (meaning that the memory footprint for message store
          // will not be more than 2*receivedBytes). Based on this assumption
          // the upper bound for coefficient of receivedBytes should be 2.
          //
          // "oocBytes" represents the size of the serialized form of data that
          // has transferred to/from secondary storage. On the other hand, in
          // our estimation mechanism, we are estimating the aggregate size of
          // all live objects in memory, meaning that we are estimating the size
          // of deserialized for of data in memory. Since we are not using any
          // (de)compression for (de)serialization of data, we assume that
          // size of serialized data <= size of deserialized data <=
          // 2 * (size of serialized dat)
          // This basically means that the coefficient for oocBytes should be
          // between 1 and 2.

          boolean changed;
          extreme[3] = -1;
          extreme[4] = -1;
          do {
            Boolean result = null;

            result = refineCoefficient(4, 1, 2, xValues, yValues);
            if (result == null) { // invalid regression result
              return;  // finally-block will release lock
            }
            changed = result;

            result = refineCoefficient(3, 0, 2, xValues, yValues);
            if (result == null) { // invalid regression result
              return;  // finally-block will release lock
            }
            changed |= result;
          } while (changed);
          if (extreme[3] != -1) {
            coefficient[3] = extreme[3];
          }
          if (extreme[4] != -1) {
            coefficient[4] = extreme[4];
          }
          setIsValid = true;
          return; // the finally-block will execute before return
        }
      } finally {
        // This inner try-finally block is necessary to ensure that the
        // lock is always released.
        try {
          isValid = setIsValid;
          printStats();
        } finally {
          lock.unlock();
        }
      }
    }

    /**
     * Certain coefficients need to be within a specific range.
     * If the coefficient is not in this range, we set it to the closest bound
     * and re-run the linear regression. In this case, we keep the possible
     * extremum coefficient in an intermediate array ("extreme"). Also, if
     * we choose the extremum coefficient for an index, that index is removed
     * from the regression calculation as well as the rest of the refinement
     * process.
     *
     * Note that the regression calculation here is based on the method of
     * least square for minimizing the error. The sum of squares of errors for
     * all points is a convex function. This means if we solve the
     * non-constrained linear regression and then refine the coefficient to
     * apply their bounds, we will achieve a solution to our constrained
     * linear regression problem.
     *
     * This method is called in a loop to refine certain coefficients. The loop
     * should continue until all coefficients are refined and are within their
     * range.
     *
     * @param coefIndex Coefficient index
     * @param lowerBound Lower bound
     * @param upperBound Upper bound
     * @param xValues double[][] matrix with data samples
     * @param yValues double[] matrix with y samples
     * @return True if coefficients were out-of-range, false otherwise. A null
     *         value means the regression result was invalid and the result of
     *         this method is invalid too.
     */
    @Nullable
    private Boolean refineCoefficient(int coefIndex, double lowerBound,
      double upperBound, double[][] xValues, double[] yValues) {

      boolean result = false;
      if (coefficient[coefIndex] < lowerBound ||
        coefficient[coefIndex] > upperBound) {

        double value;
        if (coefficient[coefIndex] < lowerBound) {
          value = lowerBound;
        } else {
          value = upperBound;
        }
        int ptr = -1;
        // Finding the 'coefIndex' in the valid indices. Since this method is
        // used only for the variables with higher indices, we use a reverse
        // loop to lookup the 'coefIndex' for faster termination of the loop.
        for (int i = validColumnIndices.size() - 1; i >= 0; --i) {
          if (validColumnIndices.get(i) == coefIndex) {
            ptr = i;
            break;
          }
        }
        if (ptr != -1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("addRecord: coefficient at index " + coefIndex +
              " is wrong in the regression, setting it to " + value);
          }
          // remove from valid column
          validColumnIndices.remove(ptr);
          // re-create the X matrix
          fillXMatrix(dataSamples, validColumnIndices, xValues);
          // adjust Y values
          for (int i = 0; i < memorySamples.size(); ++i) {
            yValues[i] -= value * dataSamples.get(i)[coefIndex];
          }
          // save new coefficient value in intermediate array
          extreme[coefIndex] = value;
          // re-run regression
          mlr.newSampleData(yValues, xValues);
          result = calculateRegression(coefficient, validColumnIndices, mlr);
          if (!result) { // invalid regression result
            return null;
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "addRecord: coefficient was not in the regression, " +
                "setting it to the extreme of the bound");
          }
          result = false;
        }
        coefficient[coefIndex] = value;
      }
      return result;
    }

    /**
     * Calculates the regression.
     * @param coefficient Array of coefficients
     * @param validColumnIndices List of valid columns
     * @param mlr {@link OLSMultipleLinearRegression} instance.
     * @return True if the result is valid, false otherwise.
     */
    private static boolean calculateRegression(double[] coefficient,
      List<Integer> validColumnIndices, OLSMultipleLinearRegression mlr) {

      if (coefficient.length != validColumnIndices.size()) {
        LOG.info("There are " + coefficient.length +
          " coefficients, and " + validColumnIndices.size() +
          " valid columns in the regression");
      }

      double[] beta = mlr.estimateRegressionParameters();
      Arrays.fill(coefficient, 0);
      for (int i = 0; i < validColumnIndices.size(); ++i) {
        coefficient[validColumnIndices.get(i)] = beta[i];
      }
      coefficient[5] = beta[validColumnIndices.size()];
      return true;
    }

    /**
     * Copies values from a List of double[] to a double[][]. Takes into
     * consideration the list of valid column indices.
     * @param sourceValues Source List of double[]
     * @param validColumnIndices Valid column indices
     * @param xValues Target double[][] matrix.
     */
    private static void fillXMatrix(List<double[]> sourceValues,
                                    List<Integer> validColumnIndices,
                                    double[][] xValues) {

      for (int i = 0; i < sourceValues.size(); ++i) {
        xValues[i] = new double[validColumnIndices.size() + 1];
        for (int j = 0; j < validColumnIndices.size(); ++j) {
          xValues[i][j] = sourceValues.get(i)[validColumnIndices.get(j)];
        }
        xValues[i][validColumnIndices.size()] = 1;
      }
    }

    /**
     * Utility function that checks whether two doubles are equals given an
     * accuracy tolerance.
     *
     * @param val1 First value
     * @param val2 Second value
     * @return True if within a threshold
     */
    private static boolean equal(double val1, double val2) {
      return Math.abs(val1 - val2) < 0.01;
    }

    /**
     * Utility function that checks if two columns have linear dependence.
     *
     * @param values Matrix in the form of a List of double[] values.
     * @param col1 First column index
     * @param col2 Second column index
     * @return True if there is linear dependence.
     */
    private static boolean isLinearDependence(List<double[]> values,
                                              int col1, int col2) {
      boolean firstValSeen = false;
      double firstVal = 0;
      for (double[] value : values) {
        double val1 = value[col1];
        double val2 = value[col2];
        if (equal(val1, 0)) {
          if (equal(val2, 0)) {
            continue;
          } else {
            return false;
          }
        }
        if (equal(val2, 0)) {
          return false;
        }
        if (!firstValSeen) {
          firstVal = val1 / val2;
          firstValSeen = true;
        } else {
          if (!equal((val1 / val2 - firstVal) / firstVal, 0)) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Prints statistics about the regression model.
     */
    private void printStats() {
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append(
          "\nEDGES\t\tVERTICES\t\tV_PROC\t\tRECEIVED\t\tOOC\t\tMEM_USED\n");
        for (int i = 0; i < dataSamples.size(); ++i) {
          for (int j = 0; j < dataSamples.get(i).length; ++j) {
            sb.append(String.format("%.2f\t\t", dataSamples.get(i)[j]));
          }
          sb.append(memorySamples.get(i));
          sb.append("\n");
        }
        sb.append("COEFFICIENT:\n");
        for (int i = 0; i < coefficient.length; ++i) {
          sb.append(String.format("%.2f\t\t", coefficient[i]));
        }
        sb.append("\n");
        LOG.debug("printStats: isValid=" + isValid + sb.toString());
      }
    }
  }
}
