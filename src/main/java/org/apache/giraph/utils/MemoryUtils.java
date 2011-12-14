package org.apache.giraph.utils;

/**
 * Helper static methods for tracking memory usage.
 */
public class MemoryUtils {
    /**
     * Get stringified runtime memory stats
     *
     * @return String of all Runtime stats.
     */
    public static String getRuntimeMemoryStats() {
        return "totalMem = " +
               (Runtime.getRuntime().totalMemory() / 1024f / 1024f) +
               "M, maxMem = "  +
               (Runtime.getRuntime().maxMemory() / 1024f / 1024f) +
               "M, freeMem = " +
               (Runtime.getRuntime().freeMemory() / 1024f / 1024f)
                + "M";
    }
}
