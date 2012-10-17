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

import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Functions for waiting on some events to happen while reporting progress */
public class ProgressableUtils {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ProgressableUtils.class);
  /** Msecs to refresh the progress meter (one minute) */
  private static final int MSEC_PERIOD = 60 * 1000;

  /** Do not instantiate. */
  private ProgressableUtils() { }

  /**
   * Wait for executor tasks to terminate, while periodically reporting
   * progress.
   *
   * @param executor Executor which we are waiting for
   * @param progressable Progressable for reporting progress (Job context)
   */
  public static void awaitExecutorTermination(ExecutorService executor,
      Progressable progressable) {
    while (!awaitExecutorTermination(executor, progressable, MSEC_PERIOD)) {
      progressable.progress();
    }
  }

  /**
   * Wait maximum given number of milliseconds for executor tasks to terminate,
   * while periodically reporting progress.
   *
   * @param executor Executor which we are waiting for
   * @param progressable Progressable for reporting progress (Job context)
   * @param remainingWaitMsecs Number of milliseconds to wait
   * @return Whether all executor tasks terminated or not
   */
  public static boolean awaitExecutorTermination(ExecutorService executor,
      Progressable progressable, int remainingWaitMsecs) {
    long timeoutTimeMsecs = System.currentTimeMillis() + remainingWaitMsecs;
    int currentWaitMsecs;
    while (true) {
      currentWaitMsecs = Math.min(remainingWaitMsecs, MSEC_PERIOD);
      try {
        if (executor.awaitTermination(currentWaitMsecs,
            TimeUnit.MILLISECONDS)) {
          return true;
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException("awaitExecutorTermination: " +
            "InterruptedException occurred while waiting for executor's " +
            "tasks to terminate", e);
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("awaitExecutorTermination: " +
            "Waiting for executor tasks to terminate " + executor.toString());
      }
      if (System.currentTimeMillis() >= timeoutTimeMsecs) {
        return false;
      }
      progressable.progress();
      remainingWaitMsecs = Math.max(0, remainingWaitMsecs - currentWaitMsecs);
    }
  }


  /**
   * Wait for the result of the future to be ready, while periodically
   * reporting progress.
   *
   * @param <T> Type of the return value of the future
   * @param future Future
   * @param progressable Progressable for reporting progress (Job context)
   * @return Computed result of the future.
   */
  public static <T> T getFutureResult(Future<T> future,
                                      Progressable progressable) {
    while (!future.isDone()) {
      tryGetFutureResult(future, progressable, MSEC_PERIOD);
    }

    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new IllegalStateException("get: " +
          "InterruptedException occurred while waiting for future result", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("get: " +
          "ExecutionException occurred while waiting for future result", e);
    }
  }

  /**
   * Wait maximum given number of milliseconds for result to become available,
   * while periodically reporting progress.
   *
   * @param <T> Type of the return value of the future
   * @param future Future
   * @param progressable Progressable for reporting progress (Job context)
   * @param msecs Number of milliseconds to wait
   * @return Future result
   */
  public static <T> T tryGetFutureResult(
      Future<T> future, Progressable progressable, int msecs) {
    long maxMsecs = System.currentTimeMillis() + msecs;
    int curMsecTimeout;
    while (true) {
      curMsecTimeout = Math.min(msecs, MSEC_PERIOD);
      try {
        future.get(curMsecTimeout, TimeUnit.MILLISECONDS);
        if (future.isDone()) {
          return future.get();
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException("tryGet: " +
            "InterruptedException occurred while waiting for future result",
            e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("tryGet: " +
            "ExecutionException occurred while waiting for future result", e);
      } catch (TimeoutException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("tryGetFutureResult: Timeout occurred");
        }
      }

      progressable.progress();
      if (System.currentTimeMillis() >= maxMsecs) {
        return null;
      }
      msecs = Math.max(0, msecs - curMsecTimeout);
    }
  }
}
