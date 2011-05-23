package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic service interface shared by both {@link CentralizedServiceMaster} and
 * {@link CentralizedServiceWorker}.
 *
 * @author aching
 */
@SuppressWarnings("rawtypes")
public interface CentralizedService<I extends WritableComparable,
                                    V extends Writable,
                                    E extends Writable,
                                    M extends Writable> {
    /**
     * Setup (must be called prior to any other function)
     */
    void setup();

    /**
     * Get the representative Vertex for this worker.  It can used to
     * call pre/post application/superstep methods defined by the user.
     *
     * @return representation vertex
     */
    Vertex<I, V, E, M> getRepresentativeVertex();

    /**
     * Get the current global superstep of the application to work on.
     *
     * @return global superstep (begins at -1)
     */
    long getSuperstep();

    /**
     * Get the manually restart superstep
     *
     * @return -1 if not manually restarted, otherwise the superstep id
     */
    long getManualRestartSuperstep();

    /**
     * Given a superstep, should it be checkpointed based on the
     * checkpoint frequency?
     *
     * @param superstep superstep to check against frequency
     * @return true if checkpoint frequency met or superstep is 1.
     */
    boolean checkpointFrequencyMet(long superstep);

    /**
     * Clean up the service (no calls may be issued after this)
     */
    void cleanup() throws IOException;
}
