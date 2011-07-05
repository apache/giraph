/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.log4j.Logger;

/**
 * A lock with a predicate that was be used to synchronize events.
 */
public class PredicateLock implements BspEvent {
    /** Lock */
    private Lock lock = new ReentrantLock();
    /** Condition associated with lock */
    private Condition cond = lock.newCondition();
    /** Predicate */
    private boolean eventOccurred = false;
    /** Class logger */
    private Logger LOG = Logger.getLogger(PredicateLock.class);

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
        if (msecs < -1) {
            throw new RuntimeException("msecs < -1");
        }

        long maxMsecs = System.currentTimeMillis() + msecs;
        long curMsecTimeout = 0;
        lock.lock();
        try {
            while (eventOccurred == false) {
                if (msecs == -1) {
                    try {
                        cond.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(
                            "waitMsecs: Caught interrupted " +
                            "exception on cond.await()", e);
                    }
                }
                else {
                    // Keep the wait non-negative
                    curMsecTimeout =
                        Math.max(maxMsecs - System.currentTimeMillis(), 0);
                    LOG.info("waitMsecs: Wait for " + curMsecTimeout);
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
                    if (System.currentTimeMillis() > maxMsecs) {
                        return false;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public void waitForever() {
        waitMsecs(-1);
    }
}
