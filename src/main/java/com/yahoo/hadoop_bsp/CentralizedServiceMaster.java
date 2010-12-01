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
     * Become the master.
     * @return true if became the master, false if the application is done.
     */
    public boolean becomeMaster();
    
	/**
	 * Create the partitioning of the index range
	 * @return number of partitions
	 */
	public int masterCreatePartitions();
	
	/**
	 * Get the current superstep
	 * @return
	 */
	public long getSuperStep();
	
	/**
	 * Master coordinates the barriers 
	 * @param superstep current superstep to barrier on
	 * @param partitions how many partitions to wait for
	 * @return true if this is the last barrier
	 */
	public boolean masterBarrier(long superstep, int partitions);
	
	/**
	 * Master determines the job state.
	 * @param state state of the job
	 */
	public void masterSetJobState(State state); 
	
	/**
	 * Master does special singular cleanup procedures (i.e. cleanup files,
	 * znodes, etc.)
	 */
	public void masterCleanup(int partitions);
}
