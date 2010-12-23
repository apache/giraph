package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.yahoo.hadoop_bsp.BspService.State;

/**
 * All workers will try to be the master as well. The master can execute the
 * following methods.
 *
 * @author aching
 *
 * @param <I>
 */
public interface CentralizedServiceMaster<I extends WritableComparable,
                                          V,
                                          E,
                                          M extends Writable> {

    /**
     * Setup (must be called prior to any other function)
     */
    public void setup();

    /**
     * Become the master.
     * @return true if became the master, false if the application is done.
     */
    public boolean becomeMaster();

    /**
     * Create the InputSplits from the index range based on the user-defined
     * VertexInputFormat.  These InputSplits will be split further into
     * partitions by the workers.
     *
     * @return number of partitions
     */
    public int createInputSplits();

    /**
     * Get the current superstep (check for the last good superstep)
     * @return
     */
    public long getSuperstep();

    /**
     * Master coordinates the superstep
     *
     * @return true if this is the last barrier (application done)
     */
    public boolean coordinateSuperstep();

    /**
     * Master determines the job state.
     * @param state state of the job
     */
    public void setJobState(State state);

    /**
     * Master does special singular cleanup procedures (i.e. cleanup files,
     * znodes, etc.)
     */
    public void cleanup();
}
