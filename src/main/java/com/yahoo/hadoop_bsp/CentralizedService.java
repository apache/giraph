package com.yahoo.hadoop_bsp;

/**
 * Basic service interface shared by both {@link CentralizedServiceMaster} and
 * {@link CentralizedServiceWorker}.
 *
 * @author aching
 */
public interface CentralizedService {
    /**
     * Setup (must be called prior to any other function)
     */
    public void setup();

    /**
     * Get the current global superstep of the application to work on.
     *
     * @return global superstep (begins at -1)
     */
    public long getSuperstep();

    /**
     * Get the manually restart superstep
     *
     * @return -1 if not manually restarted, otherwise the superstep id
     */
    public long getManualRestartSuperstep();

    /**
     * Given a superstep, should it be checkpointed based on the
     * checkpoint frequency?
     *
     * @param superstep superstep to check against frequency
     * @return true if checkpoint frequency met or superstep is 1.
     */
    public boolean checkpointFrequencyMet(long superstep);

    /**
     * Clean up the service (no calls may be issued after this)
     */
    void cleanup();
}
