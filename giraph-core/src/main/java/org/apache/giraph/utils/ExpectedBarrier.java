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

/**
 * User must follow this protocol for concurrent access:
 *
 * (1) an object instance is constructed
 * (2) arbitrarily many times
 *     (2a) concurrent calls to requirePermits(), releasePermits() and
 *          waitForRequiredPermits() are issued
 *     (2b) waitForRequiredPermits() returns
 *
 * Note that the next cycle of  calls to requirePermits() or releasePermits()
 * cannot start until the previous call to waitForRequiredPermits()
 * has returned.
 *
 * Methods of this class are thread-safe.
 */
public class ExpectedBarrier {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ExpectedBarrier.class);
  /** Msecs to refresh the progress meter */
  private static final int MSEC_PERIOD = 10000;
  /** Progressable for reporting progress */
  private final Progressable progressable;
  /** Number of times permits were added */
  private long timesRequired = 0;
  /** Number of permits we are currently waiting for */
  private long waitingOnPermits = 0;
  /** Logger */
  private final TimedLogger logger;

  /**
   * Constructor
   *
   * @param progressable Progressable for reporting progress
   */
  public ExpectedBarrier(Progressable progressable) {
    this.progressable = progressable;
    logger = new TimedLogger(MSEC_PERIOD, LOG);
  }

  /**
   * Wait until permits have been required desired number of times,
   * and all required permits are available
   *
   * @param desiredTimesRequired How many times should permits have been
   *                             required
   */
  public synchronized void waitForRequiredPermits(
      long desiredTimesRequired) {
    while (timesRequired < desiredTimesRequired || waitingOnPermits > 0) {
      try {
        wait(MSEC_PERIOD);
      } catch (InterruptedException e) {
        throw new IllegalStateException("waitForRequiredPermits: " +
            "InterruptedException occurred");
      }
      progressable.progress();
      if (LOG.isInfoEnabled()) {
        if (timesRequired < desiredTimesRequired) {
          logger.info("waitForRequiredPermits: " +
              "Waiting for times required to be " + desiredTimesRequired +
              " (currently " + timesRequired + ") ");
        } else {
          logger.info("waitForRequiredPermits: " +
              "Waiting for " + waitingOnPermits + " more permits.");
        }
      }
    }

    // Reset for the next time to use
    timesRequired = 0;
    waitingOnPermits = 0;
  }

  /**
   * Require more permits. This will increase the number of times permits
   * were required. Doesn't wait for permits to become available.
   *
   * @param permits Number of permits to require
   */
  public synchronized void requirePermits(long permits) {
    timesRequired++;
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
