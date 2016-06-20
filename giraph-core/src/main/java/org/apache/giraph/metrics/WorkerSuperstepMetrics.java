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

package org.apache.giraph.metrics;

import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.OutOfCoreIOCallable;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;

import com.yammer.metrics.core.Gauge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Per-superstep metrics for a Worker.
 */
public class WorkerSuperstepMetrics implements Writable {
  /** Total network communication time */
  private LongAndTimeUnit commTimer;
  /** Time for all compute calls to complete */
  private LongAndTimeUnit computeAllTimer;
  /** Time till first message gets flushed */
  private LongAndTimeUnit timeToFirstMsg;
  /** Total superstep time */
  private LongAndTimeUnit superstepTimer;
  /** Time spent waiting for other workers to finish */
  private LongAndTimeUnit waitRequestsTimer;
  /** Time spent doing GC in a superstep */
  private LongAndTimeUnit superstepGCTimer;
  /** Number of bytes loaded from disk to memory in out-of-core mechanism */
  private long bytesLoadedFromDisk;
  /** Number of bytes stored from memory to disk in out-of-core mechanism */
  private long bytesStoredOnDisk;
  /** Percentage of graph kept in memory */
  private double graphPercentageInMemory;

  /**
   * Constructor
   */
  public WorkerSuperstepMetrics() {
    commTimer = new LongAndTimeUnit();
    computeAllTimer = new LongAndTimeUnit();
    timeToFirstMsg = new LongAndTimeUnit();
    superstepTimer = new LongAndTimeUnit();
    waitRequestsTimer = new LongAndTimeUnit();
    superstepGCTimer = new LongAndTimeUnit();
    superstepGCTimer.setTimeUnit(TimeUnit.MILLISECONDS);
    bytesLoadedFromDisk = 0;
    bytesStoredOnDisk = 0;
    graphPercentageInMemory = 100;
  }

  /**
   * Read metric values from global MetricsRegistry.
   *
   * @return this object, for chaining
   */
  public WorkerSuperstepMetrics readFromRegistry() {
    readGiraphTimer(GraphTaskManager.TIMER_COMMUNICATION_TIME, commTimer);
    readGiraphTimer(GraphTaskManager.TIMER_COMPUTE_ALL, computeAllTimer);
    readGiraphTimer(GraphTaskManager.TIMER_TIME_TO_FIRST_MSG, timeToFirstMsg);
    readGiraphTimer(GraphTaskManager.TIMER_SUPERSTEP_TIME, superstepTimer);
    readGiraphTimer(BspServiceWorker.TIMER_WAIT_REQUESTS, waitRequestsTimer);
    SuperstepMetricsRegistry registry = GiraphMetrics.get().perSuperstep();
    superstepGCTimer.setValue(
        registry.getCounter(GraphTaskManager.TIMER_SUPERSTEP_GC_TIME).count());
    bytesLoadedFromDisk =
        registry.getCounter(OutOfCoreIOCallable.BYTES_LOAD_FROM_DISK).count();
    bytesStoredOnDisk =
        registry.getCounter(OutOfCoreIOCallable.BYTES_STORE_TO_DISK).count();
    Gauge<Double> gauge =
        registry.getExistingGauge(OutOfCoreEngine.GRAPH_PERCENTAGE_IN_MEMORY);
    if (gauge != null) {
      graphPercentageInMemory = gauge.value();
    }
    return this;
  }

  /**
   * Read data from GiraphTimer into a LongAndTimeUnit.
   *
   * @param name String name of Gauge to retrieve.
   * @param data LongAndTimeUnit to read data into.
   */
  private void readGiraphTimer(String name, LongAndTimeUnit data) {
    Gauge<Long> gauge = GiraphMetrics.get().perSuperstep().
        getExistingGauge(name);
    if (gauge instanceof GiraphTimer) {
      GiraphTimer giraphTimer = (GiraphTimer) gauge;
      data.setTimeUnit(giraphTimer.getTimeUnit());
      data.setValue(giraphTimer.value());
    } else if (gauge != null) {
      throw new IllegalStateException(name + " is not a GiraphTimer");
    }
  }

  /**
   * Human readable dump of metrics stored here.
   *
   * @param superstep long number of superstep.
   * @param out PrintStream to write to.
   * @return this object, for chaining
   */
  public WorkerSuperstepMetrics print(long superstep, PrintStream out) {
    out.println();
    out.println("--- METRICS: superstep " + superstep + " ---");
    out.println("  superstep time: " + superstepTimer);
    out.println("  compute all partitions: " + computeAllTimer);
    out.println("  time spent in gc: " + superstepGCTimer);
    out.println("  bytes transferred in out-of-core: " +
        (bytesLoadedFromDisk + bytesStoredOnDisk));
    out.println("  network communication time: " + commTimer);
    out.println("  time to first message: " + timeToFirstMsg);
    out.println("  wait on requests time: " + waitRequestsTimer);
    return this;
  }

  /**
   * @return Communication timer
   */
  public long getCommTimer() {
    return commTimer.getValue();
  }

  /**
   * @return Total compute timer
   */
  public long getComputeAllTimer() {
    return computeAllTimer.getValue();
  }

  /**
   * @return timer between start time and first message flushed.
   */
  public long getTimeToFirstMsg() {
    return timeToFirstMsg.getValue();
  }

  /**
   * @return timer for superstep time
   */
  public long getSuperstepTimer() {
    return superstepTimer.getValue();
  }

  /**
   * @return timer waiting for other workers
   */
  public long getWaitRequestsTimer() {
    return waitRequestsTimer.getValue();
  }

  /**
   * @return number of bytes loaded from disk by out-of-core mechanism (if any
   *         is used)
   */
  public long getBytesLoadedFromDisk() {
    return bytesLoadedFromDisk;
  }

  /**
   * @return number of bytes stored on disk by out-of-core mechanism (if any is
   *         used)
   */
  public long getBytesStoredOnDisk() {
    return bytesStoredOnDisk;
  }

  /**
   * @return a rough estimate of percentage of graph in memory
   */
  public double getGraphPercentageInMemory() {
    return graphPercentageInMemory;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    commTimer.setValue(dataInput.readLong());
    computeAllTimer.setValue(dataInput.readLong());
    timeToFirstMsg.setValue(dataInput.readLong());
    superstepTimer.setValue(dataInput.readLong());
    waitRequestsTimer.setValue(dataInput.readLong());
    bytesLoadedFromDisk = dataInput.readLong();
    bytesStoredOnDisk = dataInput.readLong();
    graphPercentageInMemory = dataInput.readDouble();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(commTimer.getValue());
    dataOutput.writeLong(computeAllTimer.getValue());
    dataOutput.writeLong(timeToFirstMsg.getValue());
    dataOutput.writeLong(superstepTimer.getValue());
    dataOutput.writeLong(waitRequestsTimer.getValue());
    dataOutput.writeLong(bytesLoadedFromDisk);
    dataOutput.writeLong(bytesStoredOnDisk);
    dataOutput.writeDouble(graphPercentageInMemory);
  }
}
