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

package org.apache.giraph.utils;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.metrics.AggregatedMetrics;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * An observer for both worker and master that periodically checks if available
 * memory on heap is below certain threshold, and if found to be the case
 * dumps jmap -histo for the process
 */
public class ReactiveJMapHistoDumper extends
  DefaultImmutableClassesGiraphConfigurable implements
  MasterObserver, WorkerObserver {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(
      ReactiveJMapHistoDumper.class);
  /** Size of mb */
  private static final int MB = 1024 * 1024;

  /** How many msec to sleep between calls */
  private int sleepMillis;
  /** How many lines of output to print */
  private int linesToPrint;
  /** How much free memory is expected */
  private int minFreeMemory;

  /** The jmap printing thread */
  private Thread thread;
  /** Halt jmap thread */
  private volatile boolean stop = false;
  /** Path to jmap*/
  private String jmapPath;

  @Override
  public void preLoad() {
    // This is called by both WorkerObserver and MasterObserver
    startSupervisorThread();
  }

  @Override
  public void postSave() {
    // This is called by both WorkerObserver and MasterObserver
    joinSupervisorThread();
  }

  @Override
  public void preApplication() {
  }

  @Override
  public void postApplication() {
  }

  /**
   * Join the supervisor thread
   */
  private void joinSupervisorThread() {
    stop = true;
    try {
      thread.join(sleepMillis + 5000);
    } catch (InterruptedException e) {
      LOG.error("Failed to join jmap thread");
    }
  }

  /**
   * Start the supervisor thread
   */
  public void startSupervisorThread() {
    stop = false;
    final Runtime runtime = Runtime.getRuntime();
    thread = ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (!stop) {
          long potentialMemory = (runtime.maxMemory() -
              runtime.totalMemory()) + runtime.freeMemory();
          if (potentialMemory / MB < minFreeMemory) {
            JMap.heapHistogramDump(linesToPrint, jmapPath);
          }
          ThreadUtils.trySleep(sleepMillis);
        }
      }
    }, "ReactiveJMapHistoDumperSupervisorThread");
  }

  @Override
  public void preSuperstep(long superstep) { }

  @Override
  public void postSuperstep(long superstep) { }

  @Override
  public void superstepMetricsUpdate(long superstep,
      AggregatedMetrics aggregatedMetrics,
      List<PartitionStats> partitionStatsList) { }

  @Override
  public void applicationFailed(Exception e) { }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    sleepMillis = GiraphConstants.JMAP_SLEEP_MILLIS.get(configuration);
    linesToPrint = GiraphConstants.JMAP_PRINT_LINES.get(configuration);
    minFreeMemory = GiraphConstants.MIN_FREE_MBS_ON_HEAP.get(configuration);
    jmapPath = GiraphConstants.JMAP_PATH.get(configuration);
  }
}
