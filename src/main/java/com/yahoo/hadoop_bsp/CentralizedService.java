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
	 */
	void barrier();
	
	/**
	 * All clients will get their own input split (exactly one per client).
	 */
	InputSplit getInputSplit();
	
	/**
	 * Get the current superstep.
	 */
	int getSuperStep();
}
