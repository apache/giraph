package com.yahoo.hadoop_bsp;

import java.util.Iterator;

public interface Vertex<V, E, M> {
	public void compute();
	public int getSuperstep();
	public V getVertexValue();
	public void setVertexValue(V vertexValue);
	public Iterator getOutEdgeIterator();
	public void voteToHalt();
}
