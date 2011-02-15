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
@SuppressWarnings("rawtypes")
public interface CentralizedServiceMaster<I extends WritableComparable,
                                          V extends Writable,
                                          E extends Writable,
                                          M extends Writable>
                                          extends CentralizedService {
    /**
     * Become the master.
     * @return true if became the master, false if the application is done.
     */
    boolean becomeMaster();

    /**
     * Create the InputSplits from the index range based on the user-defined
     * VertexInputFormat.  These InputSplits will be split further into
     * partitions by the workers.
     *
     * @return number of partitions
     */
    int createInputSplits();

    /**
     * Master coordinates the superstep
     *
     * @return true if this is the last barrier (application done)
     */
    boolean coordinateSuperstep();

    /**
     * Master determines the job state.
     * @param state state of the job
     */
    void setJobState(State state);
}
