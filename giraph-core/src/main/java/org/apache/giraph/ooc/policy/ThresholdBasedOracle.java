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
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.checkState;

/**
 * Out-of-core oracle to adaptively control data kept in memory, with the goal
 * of keeping the memory usage at a desired state. Out-of-core policy in this
 * oracle is based on several user-defined thresholds. Also, this oracle spawns
 * a thread to periodically check the memory usage. This thread would issue
 * manual GC calls if JVM fails to call major/full GC for a while and the amount
 * of used memory is about to cause high-memory pressure. This oracle, also,
 * monitors GC activities. The monitoring mechanism looks for major/full GC
 * calls, and updates out-of-core decisions based on the amount of available
 * memory after such GCs. There are three out-of-core decisions:
 *  - Which IO operations should be done (load/offload of partitions and
 *    messages)
 *  - What the incoming messages rate should be (updating credits announced by
 *    this worker in credit-based flow-control mechanism)
 *  - How many processing threads should remain active (tethering rate of
 *    data generation)
 *
 * The following table shows the relationship of these decisions and
 * used-defined thresholds.
 * --------------------------------------------------------------
 * Memory Pressure     |  Manual |   IO   | Credit   | Active   |
 * (memory usage)      |   GC?   | Action |          | Threads  |
 * --------------------------------------------------------------
 *                     |  Yes    | hard   |  0       |  0       |
 *                     |         | store  |          |          |
 * failPressure -------------------------------------------------
 *                     |  Yes    | hard   |  0       | fraction |
 *                     |         | store  |          |          |
 * emergencyPressure --------------------------------------------
 *                     |  Yes    | hard   | fraction |  max     |
 *                     |         | store  |          |          |
 * highPressure -------------------------------------------------
 *                     |  No     | soft   | fraction |  max     |
 *                     |         | store  |          |          |
 * optimalPressure ----------------------------------------------
 *                     |  No     | soft   |  max     |  max     |
 *                     |         | load   |          |          |
 * lowPressure --------------------------------------------------
 *                     |  No     | hard   |  max     |  max     |
 *                     |         | load   |          |          |
 * --------------------------------------------------------------
 *
 */
public class ThresholdBasedOracle implements OutOfCoreOracle {
  /** The memory pressure at/above which the job would fail */
  public static final FloatConfOption FAIL_MEMORY_PRESSURE =
      new FloatConfOption("giraph.threshold.failPressure", 0.975f,
          "The memory pressure (fraction of used memory) at/above which the " +
              "job would fail.");
  /**
   * The memory pressure at which the job is cloe to fail, even though we were
   * using maximal disk bandwidth and minimal network rate. We should reduce
   * job processing rate.
   */
  public static final FloatConfOption EMERGENCY_MEMORY_PRESSURE =
      new FloatConfOption("giraph.threshold.emergencyPressure", 0.925f,
          "The memory pressure (fraction of used memory) at which the job " +
              "is close to fail, hence we should reduce its processing rate " +
              "as much as possible.");
  /** The memory pressure at which the job is suffering from GC overhead. */
  public static final FloatConfOption HIGH_MEMORY_PRESSURE =
      new FloatConfOption("giraph.threshold.highPressure", 0.875f,
          "The memory pressure (fraction of used memory) at which the job " +
              "is suffering from GC overhead.");
  /**
   * The memory pressure at which we expect GC to perform optimally for a
   * memory intensive job.
   */
  public static final FloatConfOption OPTIMAL_MEMORY_PRESSURE =
      new FloatConfOption("giraph.threshold.optimalPressure", 0.8f,
          "The memory pressure (fraction of used memory) at which a " +
              "memory-intensive job shows the optimal GC behavior.");
  /**
   * The memory pressure at/below which the job can use more memory without
   * suffering from GC overhead.
   */
  public static final FloatConfOption LOW_MEMORY_PRESSURE =
      new FloatConfOption("giraph.threshold.lowPressure", 0.7f,
          "The memory pressure (fraction of used memory) at/below which the " +
              "job can use more memory without suffering the performance.");
  /** The interval at which memory observer thread wakes up. */
  public static final LongConfOption CHECK_MEMORY_INTERVAL =
      new LongConfOption("giraph.threshold.checkMemoryInterval", 2500,
          "The interval/period where memory observer thread wakes up and " +
              "monitors memory footprint (in milliseconds)");
  /**
   * Memory observer thread would manually call GC if major/full GC has not
   * been called for a while. The period where we expect GC to be happened in
   * past is specified in this parameter
   */
  public static final LongConfOption LAST_GC_CALL_INTERVAL =
      new LongConfOption("giraph.threshold.lastGcCallInterval", 10 * 1000,
          "How long after last major/full GC should we call manual GC?");

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ThresholdBasedOracle.class);
  /** Cached value for FAIL_MEMORY_PRESSURE */
  private final float failMemoryPressure;
  /** Cached value for EMERGENCY_MEMORY_PRESSURE */
  private final float emergencyMemoryPressure;
  /** Cached value for HIGH_MEMORY_PRESSURE */
  private final float highMemoryPressure;
  /** Cached value for OPTIMAL_MEMORY_PRESSURE */
  private final float optimalMemoryPressure;
  /** Cached value for LOW_MEMORY_PRESSURE */
  private final float lowMemoryPressure;
  /** Cached value for CHECK_MEMORY_INTERVAL */
  private final long checkMemoryInterval;
  /** Cached value for LAST_GC_CALL_INTERVAL */
  private final long lastGCCallInterval;
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Last time a major/full GC has been called (in milliseconds) */
  private volatile long lastMajorGCTime;
  /** Last time a non major/full GC has been called (in milliseconds) */
  private volatile long lastMinorGCTime;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param oocEngine out-of-core engine
   */
  public ThresholdBasedOracle(ImmutableClassesGiraphConfiguration conf,
                              OutOfCoreEngine oocEngine) {
    this.failMemoryPressure = FAIL_MEMORY_PRESSURE.get(conf);
    this.emergencyMemoryPressure = EMERGENCY_MEMORY_PRESSURE.get(conf);
    this.highMemoryPressure = HIGH_MEMORY_PRESSURE.get(conf);
    this.optimalMemoryPressure = OPTIMAL_MEMORY_PRESSURE.get(conf);
    this.lowMemoryPressure = LOW_MEMORY_PRESSURE.get(conf);
    this.checkMemoryInterval = CHECK_MEMORY_INTERVAL.get(conf);
    this.lastGCCallInterval = LAST_GC_CALL_INTERVAL.get(conf);
    NettyClient.LIMIT_OPEN_REQUESTS_PER_WORKER.setIfUnset(conf, true);
    boolean useCredit = NettyClient.LIMIT_OPEN_REQUESTS_PER_WORKER.get(conf);
    checkState(useCredit, "ThresholdBasedOracle: credit-based flow control " +
        "must be enabled. Use giraph.waitForPerWorkerRequests=true");
    this.oocEngine = oocEngine;
    this.lastMajorGCTime = 0;

    ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          double usedMemoryFraction = 1 - MemoryUtils.freeMemoryFraction();
          long time = System.currentTimeMillis();
          if ((usedMemoryFraction > highMemoryPressure &&
              time - lastMajorGCTime >= lastGCCallInterval) ||
              (usedMemoryFraction > optimalMemoryPressure &&
                  time - lastMajorGCTime >= lastGCCallInterval &&
                  time - lastMinorGCTime >= lastGCCallInterval)) {
            if (LOG.isInfoEnabled()) {
              LOG.info("call: last GC happened a while ago and the " +
                  "amount of used memory is high (used memory " +
                  "fraction is " +
                  String.format("%.2f", usedMemoryFraction) + "). " +
                  "Calling GC manually");
            }
            System.gc();
            time = System.currentTimeMillis() - time;
            usedMemoryFraction = 1 - MemoryUtils.freeMemoryFraction();
            if (LOG.isInfoEnabled()) {
              LOG.info("call: manual GC is done. It took " +
                  String.format("%.2f", (double) time / 1000) +
                  " seconds. Used memory fraction is " +
                  String.format("%.2f", usedMemoryFraction));
            }
          }
          updateRates(usedMemoryFraction);
          try {
            Thread.sleep(checkMemoryInterval);
          } catch (InterruptedException e) {
            LOG.warn("run: exception occurred!", e);
            return;
          }
        }
      }
    }, "memory-checker", oocEngine.getServiceWorker().getGraphTaskManager().
        createUncaughtExceptionHandler());
  }

  /**
   * Update statistics and rate regarding communication credits and number of
   * active threads.
   *
   * @param usedMemoryFraction the fraction of used memory over max memory
   */
  public void updateRates(double usedMemoryFraction) {
    // Update the fraction of processing threads that should remain active
    if (usedMemoryFraction >= failMemoryPressure) {
      oocEngine.updateActiveThreadsFraction(0);
    } else if (usedMemoryFraction < emergencyMemoryPressure) {
      oocEngine.updateActiveThreadsFraction(1);
    } else {
      oocEngine.updateActiveThreadsFraction(1 -
          (usedMemoryFraction - emergencyMemoryPressure) /
              (failMemoryPressure - emergencyMemoryPressure));
    }

    // Update the fraction of credit that should be used in credit-based flow-
    // control
    if (usedMemoryFraction >= emergencyMemoryPressure) {
      oocEngine.updateRequestsCreditFraction(0);
    } else if (usedMemoryFraction < optimalMemoryPressure) {
      oocEngine.updateRequestsCreditFraction(1);
    } else {
      oocEngine.updateRequestsCreditFraction(1 -
          (usedMemoryFraction - optimalMemoryPressure) /
              (emergencyMemoryPressure - optimalMemoryPressure));
    }
  }

  @Override
  public IOAction[] getNextIOActions() {
    double usedMemoryFraction = 1 - MemoryUtils.freeMemoryFraction();
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("getNextIOActions: usedMemoryFraction = %.2f",
          usedMemoryFraction));
    }
    if (usedMemoryFraction > highMemoryPressure) {
      return new IOAction[]{
        IOAction.STORE_MESSAGES_AND_BUFFERS,
        IOAction.STORE_PARTITION};
    } else if (usedMemoryFraction > optimalMemoryPressure) {
      return new IOAction[]{
        IOAction.LOAD_UNPROCESSED_PARTITION,
        IOAction.STORE_MESSAGES_AND_BUFFERS,
        IOAction.STORE_PROCESSED_PARTITION};
    } else if (usedMemoryFraction > lowMemoryPressure) {
      return new IOAction[]{
        IOAction.LOAD_UNPROCESSED_PARTITION,
        IOAction.STORE_MESSAGES_AND_BUFFERS,
        IOAction.LOAD_PARTITION};
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
    // Do nothing
  }

  @Override
  public void gcCompleted(GarbageCollectionNotificationInfo gcInfo) {
    String gcAction = gcInfo.getGcAction().toLowerCase();
    if (gcAction.contains("full") || gcAction.contains("major")) {
      if (!gcInfo.getGcCause().contains("No GC")) {
        lastMajorGCTime = System.currentTimeMillis();
      }
    } else {
      lastMinorGCTime = System.currentTimeMillis();
    }
  }

  @Override
  public void startIteration() {
  }
}
