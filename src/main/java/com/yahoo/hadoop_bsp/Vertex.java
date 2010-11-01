package com.yahoo.hadoop_bsp;

import java.util.Iterator;

/**
 * 
 * @author aching
 *
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
public interface Vertex<V, E, M> {
	/**
	 * Must be defined by user to do computation on a single Vertex.
	 */
	public void compute();
	/**
	 * Retrieves the BSP superstep.
	 * @return BSP superstep
	 */
	public int getSuperstep();
	/**
	 * Get the vertex data
	 * @return vertex data
	 */
	public V getVertexValue();
	/**
	 * Set the vertex data (immediately visible in the computation)
	 * @param vertexValue Vertex data to be set
	 */
	public void setVertexValue(V vertexValue);
	/**
	 * Every vertex has edges to other vertices.  Get a handle to the outward vertices.
	 * @return iterator to the outward edges
	 */
	public Iterator getOutEdgeIterator();
	/**
	 * After this is called, the compute() code will no longer be called for
	 * this vertice unless a message is sent to it.  Then the compute() code
	 * will be called once again until this function is called.  The application
	 * finishes only when all vertices vote to halt.
	 */
	public void voteToHalt();
}
