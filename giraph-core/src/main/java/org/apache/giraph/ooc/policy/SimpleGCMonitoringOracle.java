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

import com.google.common.collect.Maps;
import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.OutOfCoreIOStatistics;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.command.WaitIOCommand;
import org.apache.log4j.Logger;

import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Out-of-core oracle to adaptively control data kept in memory, with the goal
 * of keeping the memory state constantly at a desired state. This oracle
 * monitors GC behavior to keep track of memory pressure.
 *
 * After each GC is done, this oracle retrieve statistics about the memory
 * pressure (memory used, max memory, and how far away memory is compared to a
 * max optimal pressure). Based on the the past 2 recent memory statistics,
 * the oracle predicts the status of the memory, and sets the rate of load/store
 * of data from/to disk. If the rate of loading data from disk is 'l', and the
 * rate of storing data to disk is 's', the rate of data injection to memory
 * from disk can be denoted as 'l-s'. This oracle determines what 'l-s' should
 * be based on the prediction of memory status.
 *
 * Assume that based on the previous GC call the memory usage at time t_0 is
 * m_0, and based on the most recent GC call the memory usage at time t_1 is
 * m_1. So, the rate of memory increase is alpha = (m_1 - m_0) / (t_1 - t_0).
 * Assume that the ideal memory pressure happens when the memory usage is
 * m_ideal. So, at time 't_2 = t_1 + (t_1 - t_0)', we want m_ideal. That means
 * the ideal rate would be beta = (m_ideal - m_1) / (t_2 - t_1). If the date
 * injection rate to memory so far was i, the new injection rate should be:
 * i_new = i - (alpha - beta)
 */
public class SimpleGCMonitoringOracle implements OutOfCoreOracle {
  /**
   * The optimal memory pressure at which GC behavior is close to ideal. This
   * fraction may be dependant on the GC strategy used for running a job, but
   * generally should not be dependent on the graph processing application.
   */
  public static final FloatConfOption OPTIMAL_MEMORY_PRESSURE =
      new FloatConfOption("giraph.optimalMemoryPressure", 0.8f,
          "The memory pressure (fraction of used memory) at which the job " +
              "shows the optimal GC behavior. This fraction may be dependent " +
              "on the GC strategy used in running the job.");

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleGCMonitoringOracle.class);
  /** Cached value for OPTIMAL_MEMORY_PRESSURE */
  private final float optimalMemoryPressure;
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Status of memory from the last GC call */
  private GCObservation lastGCObservation;
  /** Desired rate of data injection to memory */
  private final AtomicLong desiredDiskToMemoryDataRate =
      new AtomicLong(0);
  /** Number of on the fly (outstanding) IO commands for each command type */
  private final Map<IOCommand.IOCommandType, AtomicInteger> commandOccurrences =
      Maps.newConcurrentMap();

  /**
   * Constructor
   *
   * @param conf configuration
   * @param oocEngine out-of-core engine
   */
  public SimpleGCMonitoringOracle(ImmutableClassesGiraphConfiguration conf,
                                  OutOfCoreEngine oocEngine) {
    this.optimalMemoryPressure = OPTIMAL_MEMORY_PRESSURE.get(conf);
    this.oocEngine = oocEngine;
    this.lastGCObservation = new GCObservation(-1, 0, 0);
    for (IOCommand.IOCommandType type : IOCommand.IOCommandType.values()) {
      commandOccurrences.put(type, new AtomicInteger(0));
    }
  }

  @Override
  public synchronized void gcCompleted(GarbageCollectionNotificationInfo
                                             gcInfo) {
    long time = System.currentTimeMillis();
    Map<String, MemoryUsage> memAfter = gcInfo.getGcInfo()
        .getMemoryUsageAfterGc();
    long usedMemory = 0;
    long maxMemory = 0;
    for (MemoryUsage memDetail : memAfter.values()) {
      usedMemory += memDetail.getUsed();
      maxMemory += memDetail.getMax();
    }
    GCObservation observation = new GCObservation(time, usedMemory, maxMemory);
    if (LOG.isInfoEnabled()) {
      LOG.info("gcCompleted: GC completed with: " + observation);
    }
    // Whether this is not the first GC call in the application
    if (lastGCObservation.isValid()) {
      long deltaDataRate =
          lastGCObservation.getDesiredDeltaDataRate(observation);
      long diskBandwidthEstimate =
          oocEngine.getIOStatistics().getDiskBandwidth();
      // Update the desired data injection rate to memory. The data injection
      // rate cannot be less than -disk_bandwidth (the extreme case happens if
      // we only do 'store'), and cannot be more than disk_bandwidth (the
      // extreme case happens if we only do 'load').
      long dataInjectionRate = desiredDiskToMemoryDataRate.get();
      desiredDiskToMemoryDataRate.set(Math.max(
          Math.min(desiredDiskToMemoryDataRate.get() - deltaDataRate,
              diskBandwidthEstimate), -diskBandwidthEstimate));
      if (LOG.isInfoEnabled()) {
        LOG.info("gcCompleted: changing data injection rate from " +
            String.format("%.2f", dataInjectionRate / 1024.0 / 1024.0) +
            " to " + String.format("%.2f", desiredDiskToMemoryDataRate.get() /
            1024.0 / 1024.0));
      }
    }
    lastGCObservation = observation;
  }

  @Override
  public void startIteration() {
  }

  /**
   * Get the current data injection rate to memory based on the commands ran
   * in the history (retrieved from statistics collector), and outstanding
   * commands issued by the IO scheduler.
   *
   * @return the current data injection rate to memory
   */
  private long getCurrentDataInjectionRate() {
    long effectiveBytesTransferred = 0;
    long effectiveDuration = 0;
    for (IOCommand.IOCommandType type : IOCommand.IOCommandType.values()) {
      OutOfCoreIOStatistics.BytesDuration stats =
          oocEngine.getIOStatistics().getCommandTypeStats(type);
      int occurrence = commandOccurrences.get(type).get();
      long typeBytesTransferred = stats.getBytes();
      long typeDuration = stats.getDuration();
      // If there is an outstanding command, we still do not know how many bytes
      // it will transfer, and how long it will take. So, we guesstimate these
      // numbers based on other similar commands happened in the history. We
      // simply take the average number of bytes transferred for the particular
      // command, and we take average duration for the particular command. We
      // should multiply these numbers by the number of outstanding commands of
      // this particular command type.
      if (stats.getOccurrence() != 0) {
        typeBytesTransferred += stats.getBytes() / stats.getOccurrence() *
            occurrence;
        typeDuration += stats.getDuration() / stats.getOccurrence() *
            occurrence;
      }
      if (type == IOCommand.IOCommandType.LOAD_PARTITION) {
        effectiveBytesTransferred += typeBytesTransferred;
      } else {
        // Store (data going out of memory), or wait (no data transferred)
        effectiveBytesTransferred -= typeBytesTransferred;
      }
      effectiveDuration += typeDuration;
    }
    if (effectiveDuration == 0) {
      return 0;
    } else {
      return effectiveBytesTransferred / effectiveDuration;
    }
  }

  @Override
  public IOAction[] getNextIOActions() {
    long error = (long) (oocEngine.getIOStatistics().getDiskBandwidth() * 0.05);
    long desiredRate = desiredDiskToMemoryDataRate.get();
    long currentRate = getCurrentDataInjectionRate();
    if (desiredRate > error) {
      // 'l-s' is positive, we should do more load than store.
      if (currentRate > desiredRate + error) {
        // We should decrease 'l-s'. This can be done either by increasing 's'
        // or issuing wait command. We prioritize wait over hard store.
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PROCESSED_PARTITION};
      } else if (currentRate < desiredRate - error) {
        // We should increase 'l-s'. We can simply load partitions/data.
        return new IOAction[]{IOAction.LOAD_PARTITION};
      } else {
        // We are in a proper state and we should keep up with the rate. We can
        // either soft store data or load data (hard load, since we desired rate
        // is positive).
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PROCESSED_PARTITION,
          IOAction.LOAD_PARTITION};
      }
    } else if (desiredRate < -error) {
      // 'l-s' is negative, we should do more store than load.
      if (currentRate < desiredRate - error) {
        // We should increase 'l-s', but we should be cautious. We only do soft
        // load, or wait.
        return new IOAction[]{IOAction.LOAD_UNPROCESSED_PARTITION};
      } else if (currentRate > desiredRate + error) {
        // We should reduce 'l-s', we do hard store.
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PARTITION};
      } else {
        // We should keep up with the rate. We can either soft store data, or
        // soft load data.
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PROCESSED_PARTITION,
          IOAction.LOAD_UNPROCESSED_PARTITION};
      }
    } else {
      // 'l-s' is almost zero. If current rate is over the desired rate, we do
      // soft store. If the current rate is below the desired rate, we do soft
      // load.
      if (currentRate > desiredRate + error) {
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PROCESSED_PARTITION};
      } else if (currentRate < desiredRate - error) {
        return new IOAction[]{IOAction.LOAD_UNPROCESSED_PARTITION};
      } else {
        return new IOAction[]{
          IOAction.STORE_MESSAGES_AND_BUFFERS,
          IOAction.STORE_PROCESSED_PARTITION,
          IOAction.LOAD_UNPROCESSED_PARTITION};
      }
    }
  }

  @Override
  public synchronized boolean approve(IOCommand command) {
    long error = (long) (oocEngine.getIOStatistics().getDiskBandwidth() * 0.05);
    long desiredRate = desiredDiskToMemoryDataRate.get();
    long currentRate = getCurrentDataInjectionRate();
    // The command is denied iff the current rate is above the desired rate and
    // we are doing load (instead of store), or the current rate is below the
    // desired rate and we are doing store (instead of loading).
    if (currentRate > desiredRate + error &&
        command instanceof LoadPartitionIOCommand) {
      return false;
    }
    if (currentRate < desiredRate - error &&
        !(command instanceof LoadPartitionIOCommand) &&
        !(command instanceof WaitIOCommand)) {
      return false;
    }
    commandOccurrences.get(command.getType()).getAndIncrement();
    return true;
  }

  @Override
  public void commandCompleted(IOCommand command) {
    commandOccurrences.get(command.getType()).getAndDecrement();
  }

  /** Helper class to record memory status after GC calls */
  private class GCObservation {
    /** The time at which the GC happened (in milliseconds) */
    private long time;
    /** Amount of memory used after the GC call */
    private long usedMemory;
    /** Maximum amounts of memory reported by GC listener */
    private long maxMemory;

    /**
     * Constructor
     *
     * @param time time of GC
     * @param usedMemory amount of used memory after GC
     * @param maxMemory amount of all available memory based on GC observation
     */
    public GCObservation(long time, long usedMemory, long maxMemory) {
      this.time = time;
      this.usedMemory = usedMemory;
      this.maxMemory = maxMemory;
    }

    /**
     * Is this a valid observation?
     *
     * @return true iff it is a valid observation
     */
    public boolean isValid() {
      return time > 0;
    }

    /**
     * Considering a new observation of memory status after the most recent GC,
     * what is the desired rate for data injection to memory.
     *
     * @param newObservation the most recent GC observation
     * @return desired rate of data injection to memory
     */
    public long getDesiredDeltaDataRate(GCObservation newObservation) {
      long newUsedMemory = newObservation.usedMemory;
      long newMaxMemory = newObservation.maxMemory;
      long lastUsedMemory = usedMemory;
      long lastMaxMemory = maxMemory;
      // Scale the memory status of two GC observation to be the same
      long scaledMaxMemory = Math.min(lastMaxMemory, newMaxMemory);
      newUsedMemory =
          (long) (((double) scaledMaxMemory / newMaxMemory) * newUsedMemory);
      lastUsedMemory =
          (long) (((double) scaledMaxMemory / lastMaxMemory) * lastUsedMemory);
      long desiredUsedMemory = (long) (optimalMemoryPressure * scaledMaxMemory);
      if (LOG.isInfoEnabled()) {
        LOG.info("getDesiredDeltaDataRate: " + String.format("previous usage " +
            "= %.2f MB, ", lastUsedMemory / 1024.0 / 1024.0) + String.format(
            "current usage = %.2f MB, ", newUsedMemory / 1024.0 / 1024.0) +
            String.format("ideal usage = %.2f MB", desiredUsedMemory / 1024.0 /
                1024.0));
      }
      long interval = newObservation.time - time;
      if (interval == 0) {
        interval = 1;
        LOG.warn("getDesiredDeltaRate: two GC happened almost at the same " +
            "time!");
      }
      long currentDataRate = (long) ((double) (newUsedMemory -
          lastUsedMemory) / interval * 1000);
      long desiredDataRate = (long) ((double) (desiredUsedMemory -
          newUsedMemory) / interval * 1000);
      return currentDataRate - desiredDataRate;
    }

    @Override
    public String toString() {
      return String.format("(usedMemory: %.2f MB, maxMemory: %.2f MB at " +
          "time: %d ms)", usedMemory / 1024.0 / 1024.0,
          maxMemory / 1024.0 / 1024.0, time);
    }
  }
}
