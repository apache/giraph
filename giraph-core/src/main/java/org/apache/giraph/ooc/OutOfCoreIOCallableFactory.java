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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.LogStacktraceCallable;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;

/**
 * Factory class to create IO threads for out-of-core engine.
 */
public class OutOfCoreIOCallableFactory {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(OutOfCoreIOCallableFactory.class);
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Result of IO threads at the end of the computation */
  private final List<Future> results;
  /** How many disks (i.e. IO threads) do we have? */
  private int numDisks;
  /** Path prefix for different disks */
  private final String[] basePaths;
  /** Executor service for IO threads */
  private ExecutorService outOfCoreIOExecutor;
  /**
   * Constructor
   *
   * @param conf Configuration
   * @param oocEngine Out-of-core engine
   */
  public OutOfCoreIOCallableFactory(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
      OutOfCoreEngine oocEngine) {
    this.oocEngine = oocEngine;
    this.results = new ArrayList<>();
    // Take advantage of multiple disks
    String[] userPaths = PARTITIONS_DIRECTORY.getArray(conf);
    this.numDisks = userPaths.length;
    this.basePaths = new String[numDisks];
    int ptr = 0;
    for (String path : userPaths) {
      File file = new File(path);
      if (!file.exists()) {
        checkState(file.mkdirs(), "OutOfCoreIOCallableFactory: cannot create " +
            "directory " + file.getAbsolutePath());
      }
      basePaths[ptr] = path + "/" + conf.get("mapred.job.id", "Unknown Job");
      ptr++;
    }
  }

  /**
   * Creates/Launches IO threads
   */
  public void createCallable() {
    CallableFactory<Void> outOfCoreIOCallableFactory =
      new CallableFactory<Void>() {
        @Override
        public Callable<Void> newCallable(int callableId) {
          return new OutOfCoreIOCallable(oocEngine, basePaths[callableId],
              callableId);
        }
      };
    outOfCoreIOExecutor = new ThreadPoolExecutor(numDisks, numDisks, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        ThreadUtils.createThreadFactory("ooc-io-%d")) {
      @Override
      protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future<?>) {
          try {
            Future<?> future = (Future<?>) r;
            if (future.isDone()) {
              future.get();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            t = e;
          }
          if (t != null) {
            LOG.info("afterExecute: an out-of-core thread terminated " +
                "unexpectedly with " + t);
            oocEngine.failTheJob();
          }
        }
      }
    };

    for (int i = 0; i < numDisks; ++i) {
      Future<Void> future = outOfCoreIOExecutor.submit(
          new LogStacktraceCallable<>(
              outOfCoreIOCallableFactory.newCallable(i)));
      results.add(future);
    }
    // Notify executor to not accept any more tasks
    outOfCoreIOExecutor.shutdown();
  }

  /**
   * How many disks do we have?
   *
   * @return number of disks (IO threads)
   */
  public int getNumDisks() {
    return numDisks;
  }

  /**
   * Check whether all IO threads terminated gracefully.
   */
  public void shutdown() {
    boolean threadsTerminated = false;
    while (!threadsTerminated) {
      if (LOG.isInfoEnabled()) {
        LOG.info("shutdown: waiting for IO threads to finish!");
      }
      try {
        threadsTerminated =
            outOfCoreIOExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException("shutdown: caught " +
            "InterruptedException while waiting for IO threads to finish");
      }
    }
    for (int i = 0; i < numDisks; ++i) {
      try {
        // Check whether the tread terminated gracefully
        results.get(i).get();
      } catch (InterruptedException e) {
        LOG.error("shutdown: IO thread " + i + " was interrupted during its " +
            "execution");
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        LOG.error("shutdown: IO thread " + i + " threw an exception during " +
            "its execution");
        throw new IllegalStateException(e);
      }
    }
    for (String path : basePaths) {
      File file = new File(path).getParentFile();
      for (String subFileName : file.list()) {
        File subFile = new File(file.getPath(), subFileName);
        checkState(subFile.delete(), "shutdown: cannot delete file %s",
            subFile.getAbsoluteFile());
      }
      checkState(file.delete(), "shutdown: cannot delete directory %s",
          file.getAbsoluteFile());
    }
  }
}
