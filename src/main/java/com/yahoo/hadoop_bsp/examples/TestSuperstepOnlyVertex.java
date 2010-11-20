package com.yahoo.hadoop_bsp.examples;

import com.yahoo.hadoop_bsp.HadoopVertex;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 * @author aching
 *
 */
public class TestSuperstepOnlyVertex extends 
	HadoopVertex<String, String, Integer, Integer> {
    public void compute() {
    	if (getSuperstep() > 3) {
    		voteToHalt();
        }
    }
}
