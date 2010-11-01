package com.yahoo.hadoop_bsp;

public final class TestBSP<V, E, M> extends HadoopVertex<V, E, M> {
    public void compute() {
    	if (getSuperstep() > 30) {
    		voteToHalt();
        }
    }
}
