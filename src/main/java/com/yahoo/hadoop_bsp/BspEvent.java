package com.yahoo.hadoop_bsp;

/**
 * Synchronize on waiting for an event to have happened.  This is a one-time
 * event.
 */
public interface BspEvent {
    /**
     * Reset the permanent signal.
     */
    void reset();

    /**
     * The event occurred and the occurrence has been logged for future
     * waiters.
     */
    void signal();

    /**
     * Wait until the event occurred or waiting timed out.
     * @param msecs Milliseconds to wait until the event occurred. 0 indicates
     *        check immediately.  -1 indicates wait forever.
     * @return true if event occurred, false if timed out while waiting
     */
    boolean waitMsecs(int msecs);

    /**
     * Wait indefinitely until the event occurs.
     */
    void waitForever();
}
