package com.yahoo.hadoop_bsp;

import com.yahoo.hadoop_bsp.BspService.State;

/**
 * The master will be required to do the following:
 * @author aching
 *
 * @param <I>
 */
public interface CentralizedServiceMaster<I> {
	/**
	 * Create the partitioning of the index range
	 * @return number of partitions
	 */
	int masterCreatePartitions();
	
	/**
	 * Get the current superstep
	 * @return
	 */
	long getSuperStep();
	
	/**
	 * Master coordinates the barriers 
	 * @param superstep current superstep to barrier on
	 * @param partitions how many partitions to wait for
	 * @return true if this is the last barrier
	 */
	boolean masterBarrier(long superstep, int partitions);
	
	/**
	 * Master determines the job state.
	 * @param state state of the job
	 */
	void masterSetJobState(State state); 
}
