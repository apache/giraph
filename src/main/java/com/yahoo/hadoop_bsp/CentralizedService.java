package com.yahoo.hadoop_bsp;

import java.util.SortedSet;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * All computational tasks should have access to this centralized service to 
 * execute the following methods.
 * @author aching
 *
 */
public interface CentralizedService<I> {
	/**
	 * Guaranteed to be called prior to any other method.
	 * @return true if jobState is FINISHED.
	 */
	boolean setup();
	
	/**
	 * Synchronizes all clients.  All clients should eventually call this, 
	 * or else it will never complete.
     * @param localVerticesDone number of vertices done on this process
     * @param localVerticesTotal number of vertices handled by this process
	 * @return true if that was the last barrier to do, false otherwise
	 */
	boolean barrier(long localVerticesDone, long localVerticesTotal);
	
	/**
	 * All clients will get their own input split (exactly one per client).
	 */
	InputSplit getInputSplit();
	
	/**
	 * Each client will set its own partition maximum.
	 */
	void setPartitionMax(I max);
	
	/**
	 * Get the partition set.
	 * @return partition set
	 */
	SortedSet<Partition<I>> getPartitionSet();
	
	/**
	 * Every client will need to get a partition for an index
	 */
	Partition<I> getPartition(I index);
	
	/**
	 * Get the current superstep.
	 * @return global superstep (begins at 0)
	 */
	long getSuperStep();
		
	/**
	 * Get the total vertices in the entire application during a given
	 * superstep.
	 * @return count of all the vertices (local and non-local)
	 */
	long getTotalVertices();
	
	/**
	 * Clean up the service (no calls may be issued after this)
	 */
	void cleanup();
}
