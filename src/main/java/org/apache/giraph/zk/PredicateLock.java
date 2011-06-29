package org.apache.giraph.zk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class PredicateLock implements BspEvent {
    /** Lock */
    Lock lock = new ReentrantLock();
    /** Condition associated with lock */
    Condition cond = lock.newCondition();
    /** Predicate */
    boolean eventOccurred = false;

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
            cond.signal();
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
                    cond.await();
                }
                else {
                    // Keep the wait non-negative
                    curMsecTimeout = Math.max(
                        maxMsecs - System.currentTimeMillis(), 0);
                    cond.await(curMsecTimeout, TimeUnit.MILLISECONDS);
                    if (System.currentTimeMillis() > maxMsecs) {
                        return false;
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
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
