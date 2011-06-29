package org.apache.giraph.zk;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * A lock that will keep the job context updated while waiting.
 */
public class ContextLock extends PredicateLock {
    /** Job context (for progress) */
    @SuppressWarnings("rawtypes")
    private final Context context;
    /** Msecs to refresh the progress meter */
    private static final int msecPeriod = 10000;

    /**
     * Constructor.
     *
     * @param context used to call progress()
     */
    ContextLock(@SuppressWarnings("rawtypes") Context context) {
        this.context = context;
    }

    /**
     * Specialized version of waitForever() that will keep the job progressing
     * while waiting.
     */
    @Override
    public void waitForever() {
        while (waitMsecs(msecPeriod) == false) {
            context.progress();
        }
    }
}
