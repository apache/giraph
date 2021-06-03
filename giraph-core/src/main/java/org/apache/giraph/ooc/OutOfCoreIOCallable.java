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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.ooc.command.IOCommand;
import org.apache.giraph.ooc.command.LoadPartitionIOCommand;
import org.apache.giraph.ooc.command.WaitIOCommand;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * IO threads for out-of-core mechanism.
 */
public class OutOfCoreIOCallable implements Callable<Void>,
    ResetSuperstepMetricsObserver {
  /** Name of Metric for number of bytes read from disk */
  public static final String BYTES_LOAD_FROM_DISK = "ooc-bytes-load";
  /** Name of Metric for number of bytes written to disk */
  public static final String BYTES_STORE_TO_DISK = "ooc-bytes-store";
  /** Name of Metric for size of loads */
  public static final String HISTOGRAM_LOAD_SIZE = "ooc-load-size-bytes";
  /** Name of Metric for size of stores */
  public static final String HISTOGRAM_STORE_SIZE = "ooc-store-size-bytes";
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(OutOfCoreIOCallable.class);
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Thread id/Disk id */
  private final int diskId;
  /** How many bytes of data is read from disk */
  private Counter bytesReadPerSuperstep;
  /** How many bytes of data is written to disk */
  private Counter bytesWrittenPerSuperstep;
  /** Size of load IO commands */
  private Histogram histogramLoadSize;
  /** Size of store IO commands */
  private Histogram histogramStoreSize;

  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param diskId thread id/disk id
   */
  public OutOfCoreIOCallable(OutOfCoreEngine oocEngine, int diskId) {
    this.oocEngine = oocEngine;
    this.diskId = diskId;
    newSuperstep(GiraphMetrics.get().perSuperstep());
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  @Override
  public Void call() throws Exception {
    while (true) {
      oocEngine.getSuperstepLock().readLock().lock();
      IOCommand command = oocEngine.getIOScheduler().getNextIOCommand(diskId);
      if (LOG.isDebugEnabled() && !(command instanceof WaitIOCommand)) {
        LOG.debug("call: thread " + diskId + "'s next IO command is: " +
            command);
      }
      if (command == null) {
        oocEngine.getSuperstepLock().readLock().unlock();
        break;
      }
      if (command instanceof WaitIOCommand) {
        oocEngine.getSuperstepLock().readLock().unlock();
      }

      boolean commandExecuted = false;
      long duration = 0;
      long bytes;
      // CHECKSTYLE: stop IllegalCatch
      try {
        long timeInGC = oocEngine.getServiceWorker().getGraphTaskManager()
            .getSuperstepGCTime();
        long startTime = System.currentTimeMillis();
        commandExecuted = command.execute();
        duration = System.currentTimeMillis() - startTime;
        timeInGC = oocEngine.getServiceWorker().getGraphTaskManager()
            .getSuperstepGCTime() - timeInGC;
        bytes = command.bytesTransferred();
        if (LOG.isDebugEnabled() && !(command instanceof WaitIOCommand)) {
          LOG.debug("call: thread " + diskId + "'s command " + command +
              " completed: bytes= " + bytes + ", duration=" + duration + ", " +
              "bandwidth=" + String.format("%.2f", (double) bytes / duration *
              1000 / 1024 / 1024) +
              ((command instanceof WaitIOCommand) ? "" :
                  (", bandwidth (excluding GC time)=" + String.format("%.2f",
                      (double) bytes / (duration - timeInGC) *
                          1000 / 1024 / 1024))));
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "call: execution of IO command " + command + " failed!", e);
      }
      // CHECKSTYLE: resume IllegalCatch
      if (!(command instanceof WaitIOCommand)) {
        oocEngine.getSuperstepLock().readLock().unlock();
        if (bytes != 0) {
          if (command instanceof LoadPartitionIOCommand) {
            bytesReadPerSuperstep.inc(bytes);
            histogramLoadSize.update(bytes);
          } else {
            bytesWrittenPerSuperstep.inc(bytes);
            histogramStoreSize.update(bytes);
          }
        }
      }

      if (commandExecuted && duration > 0) {
        oocEngine.getIOStatistics().update(command.getType(),
            command.bytesTransferred(), duration);
      }
      oocEngine.getIOScheduler().ioCommandCompleted(command);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("call: out-of-core IO thread " + diskId + " terminating!");
    }
    return null;
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    bytesReadPerSuperstep = superstepMetrics.getCounter(BYTES_LOAD_FROM_DISK);
    bytesWrittenPerSuperstep =
        superstepMetrics.getCounter(BYTES_STORE_TO_DISK);
    histogramLoadSize =
        superstepMetrics.getUniformHistogram(HISTOGRAM_LOAD_SIZE);
    histogramStoreSize =
        superstepMetrics.getUniformHistogram(HISTOGRAM_STORE_SIZE);
  }
}

