package com.yahoo.hadoop_bsp;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * A lock that will keep the job context updated while waiting.
 *
 * @author aching
 */
public class ContextLock extends PredicateLock {
    /** Job context (for progress) */
    @SuppressWarnings("rawtypes")
    private final Context m_context;
    /** Msecs to refresh the progress meter */
    private static final int m_msecPeriod = 10000;

    /**
     * Constructor.
     *
     * @param context used to call progress()
     */
    ContextLock(@SuppressWarnings("rawtypes") Context context) {
        m_context = context;
    }

    /**
     * Specialized version of waitForever() that will keep the job progressing
     * while waiting.
     */
    @Override
    public void waitForever() {
        while (waitMsecs(m_msecPeriod) == false) {
            m_context.progress();
        }
    }
}
