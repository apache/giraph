package com.yahoo.hadoop_bsp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PredicateLock implements BspEvent {
    /** Lock */
    Lock m_lock = new ReentrantLock();
    /** Condition associated with m_lock */
    Condition m_cond = m_lock.newCondition();
    /** Predicate */
    boolean m_eventOccurred = false;

    public void reset() {
        m_lock.lock();
        try {
            m_eventOccurred = false;
        } finally {
            m_lock.unlock();
        }
    }

    public void signal() {
        m_lock.lock();
        try {
            m_eventOccurred = true;
            m_cond.signal();
        } finally {
            m_lock.unlock();
        }
    }

    public boolean waitMsecs(int msecs) {
        if (msecs < -1) {
            throw new RuntimeException("msecs < -1");
        }

        long maxMsecs = System.currentTimeMillis() + msecs;
        long curMsecTimeout = 0;
        m_lock.lock();
        try {
            while (m_eventOccurred == false) {
                if (msecs == -1) {
                    m_cond.await();
                }
                else {
                    // Keep the wait non-negative
                    curMsecTimeout = Math.max(
                        maxMsecs - System.currentTimeMillis(), 0);
                    m_cond.await(curMsecTimeout, TimeUnit.MILLISECONDS);
                    if (System.currentTimeMillis() > maxMsecs) {
                        return false;
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }
        return true;
    }

    public void waitForever() {
        waitMsecs(-1);
    }
}
