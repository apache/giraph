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

package org.apache.giraph.zk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

/**
 * A lock with a predicate that was be used to synchronize events and keep the
 * job context updated while waiting.
 */
public class PredicateLock implements BspEvent {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(PredicateLock.class);
  /** Default msecs to refresh the progress meter */
  private static final int DEFAULT_MSEC_PERIOD = 10000;
  /** Progressable for reporting progress (Job context) */
  protected final Progressable progressable;
  /** Actual mses to refresh the progress meter */
  private final int msecPeriod;
  /** Lock */
  private Lock lock = new ReentrantLock();
  /** Condition associated with lock */
  private Condition cond = lock.newCondition();
  /** Predicate */
  private boolean eventOccurred = false;
  /** Keeps track of the time */
  private final Time time;

  /**
   * Constructor with default values.
   *
   * @param progressable used to report progress() (usually a Mapper.Context)
   */
  public PredicateLock(Progressable progressable) {
    this(progressable, DEFAULT_MSEC_PERIOD, SystemTime.get());
  }

  /**
   * Constructor.
   *
   * @param progressable used to report progress() (usually a Mapper.Context)
   * @param msecPeriod Msecs between progress reports
   * @param time Time implementation
   */
  public PredicateLock(Progressable progressable, int msecPeriod, Time time) {
    this.progressable = progressable;
    this.msecPeriod = msecPeriod;
    this.time = time;
  }

  @Override
  public void reset() {
    lock.lock();
    try {
      eventOccurred = false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void signal() {
    lock.lock();
    try {
      eventOccurred = true;
      cond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean waitMsecs(int msecs) {
    if (msecs < 0) {
      throw new RuntimeException("waitMsecs: msecs cannot be negative!");
    }
    long maxMsecs = time.getMilliseconds() + msecs;
    int curMsecTimeout = 0;
    lock.lock();
    try {
      while (!eventOccurred) {
        curMsecTimeout =
            Math.min(msecs, msecPeriod);
        if (LOG.isDebugEnabled()) {
          LOG.debug("waitMsecs: Wait for " + curMsecTimeout);
        }
        try {
          boolean signaled =
              cond.await(curMsecTimeout, TimeUnit.MILLISECONDS);
          if (LOG.isDebugEnabled()) {
            LOG.debug("waitMsecs: Got timed signaled of " +
              signaled);
          }
        } catch (InterruptedException e) {
          throw new IllegalStateException(
            "waitMsecs: Caught interrupted " +
            "exception on cond.await() " +
            curMsecTimeout, e);
        }
        if (time.getMilliseconds() > maxMsecs) {
          return false;
        }
        msecs = Math.max(0, msecs - curMsecTimeout);
        progressable.progress(); // go around again
      }
    } finally {
      lock.unlock();
    }
    return true;
  }

  @Override
  public void waitForTimeoutOrFail(long timeout) {
    long t0 = System.currentTimeMillis();
    while (!waitMsecs(msecPeriod)) {
      if (System.currentTimeMillis() > t0 + timeout) {
        throw new RuntimeException("Timeout waiting");
      }
      progressable.progress();
    }
  }
}
