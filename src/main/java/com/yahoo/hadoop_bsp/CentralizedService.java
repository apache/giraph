package com.yahoo.hadoop_bsp;

import org.apache.hadoop.mapreduce.InputSplit;

/**
 * All computational tasks should have access to this centralized service to 
 * execute the following methods.
 * @author aching
 *
 */
public interface CentralizedService {
	/**
	 * Guaranteed to be called prior to any other method.
	 */
	void setup();
	
	/**
	 * Synchronizes all clients.  All clients should eventually call this, 
	 * or else it will never complete.
	 * @param done true if the vertices for this process are complete, false
	 *        otherwise
	 * @return true if that was the last barrier to do, false otherwise
	 */
	boolean barrier(long verticesDone, long verticesTotal);
	
	/**
	 * All clients will get their own input split (exactly one per client).
	 */
	InputSplit getInputSplit();
	
	/**
	 * Get the current superstep.
	 * @return global superstep (begins at 0)
	 */
	long getSuperStep();
		
	/**
	 * Clean up the service (no calls may be issued after this)
	 */
	void cleanup();
}
