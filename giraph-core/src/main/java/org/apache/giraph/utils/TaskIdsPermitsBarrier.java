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

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

/**
 * This barrier is used when we don't know how many events are we waiting on
 * from the start. Instead we have a set of task ids, and each of those will,
 * at some point of time, give the information about how many events from it
 * should we expect. Barrier will be waiting for all the tasks to notify it
 * about that number of events, and than it will also wait for all the events
 * to happen.
 *
 * requirePermits() corresponds to task notifying us how many events from it
 * to expect, and releasePermits() notifies us about events happening.
 *
 * This class is currently used during preparation of aggregators.
 *
 * User must follow this protocol for concurrent access:
 *
 * (1) an object instance is constructed
 * (2) arbitrarily many times
 *     (2a) concurrent calls to requirePermits(), releasePermits() and
 *          waitForRequiredPermits() are issued
 *     (2b) waitForRequiredPermits() returns
 *
 * Note that the next cycle of calls to requirePermits() or releasePermits()
 * cannot start until the previous call to waitForRequiredPermits()
 * has returned.
 *
 * Methods of this class are thread-safe.
 */
public class TaskIdsPermitsBarrier {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TaskIdsPermitsBarrier.class);
  /** Msecs to refresh the progress meter */
  private static final int MSEC_PERIOD = 10000;
  /** Maximum number of task ids to list in the log */
  private static final int MAX_TASK_IDS_TO_LOG = 10;
  /** Progressable for reporting progress */
  private final Progressable progressable;
  /** Number of permits we are currently waiting for */
  private long waitingOnPermits = 0;
  /** Set of task ids which required permits already */
  private final Set<Integer> arrivedTaskIds = new HashSet<Integer>();
  /** Logger */
  private final TimedLogger logger;

  /**
   * Constructor
   *
   * @param progressable Progressable for reporting progress
   */
  public TaskIdsPermitsBarrier(Progressable progressable) {
    this.progressable = progressable;
    logger = new TimedLogger(MSEC_PERIOD, LOG);
  }

  /**
   * Wait until permits have been required desired number of times,
   * and all required permits are available
   *
   * @param expectedTaskIds List of task ids which we are waiting permits from
   */
  public synchronized void waitForRequiredPermits(
      Set<Integer> expectedTaskIds) {
    while (arrivedTaskIds.size() < expectedTaskIds.size() ||
        waitingOnPermits > 0) {
      try {
        wait(MSEC_PERIOD);
      } catch (InterruptedException e) {
        throw new IllegalStateException("waitForRequiredPermits: " +
            "InterruptedException occurred");
      }
      progressable.progress();
      if (LOG.isInfoEnabled()) {
        if (arrivedTaskIds.size() < expectedTaskIds.size()) {
          String logSuffix = "";
          if (expectedTaskIds.size() - arrivedTaskIds.size() <=
              MAX_TASK_IDS_TO_LOG) {
            Sets.SetView<Integer> difference =
                Sets.difference(expectedTaskIds, arrivedTaskIds);
            logSuffix = ", task ids: " + difference;
          }
          logger.info("waitForRequiredPermits: " +
              "Waiting for " +
              (expectedTaskIds.size() - arrivedTaskIds.size()) +
              " more tasks to send their aggregator data" +
              logSuffix);
        } else {
          logger.info("waitForRequiredPermits: " +
              "Waiting for " + waitingOnPermits + " more aggregator requests");
        }
      }
    }

    // Reset for the next time to use
    arrivedTaskIds.clear();
    waitingOnPermits = 0;
  }

  /**
   * Require more permits. This will increase the number of times permits
   * were required. Doesn't wait for permits to become available.
   *
   * @param permits Number of permits to require
   * @param taskId Task id which required permits
   */
  public synchronized void requirePermits(long permits, int taskId) {
    arrivedTaskIds.add(taskId);
    waitingOnPermits += permits;
    notifyAll();
  }

  /**
   * Release one permit.
   */
  public synchronized void releaseOnePermit() {
    releasePermits(1);
  }

  /**
   * Release some permits.
   *
   * @param permits Number of permits to release
   */
  public synchronized void releasePermits(long permits) {
    waitingOnPermits -= permits;
    notifyAll();
  }
}
