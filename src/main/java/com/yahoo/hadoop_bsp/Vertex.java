package com.yahoo.hadoop_bsp;

import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;

/**
 * Basic interface for writing a BSP application for computation.
 * 
 * @author aching
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
public interface Vertex<I extends WritableComparable, V, E, M> {
	/**
	 * Must be defined by user to do computation on a single Vertex.
	 */
	public void compute(Iterator<M> msgIterator);
	
	/**
	 * Retrieves the BSP superstep.
	 * @return BSP superstep
	 */
	public long getSuperstep();
	
	/**
	 * Get the vertex id
	 */
	public I getVertexId();
	
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
	 * Get the total number of vertices
	 * @return total number of vertices
	 */
	public long getNumVertices();
	
	/**
	 * Every vertex has edges to other vertices.  Get a handle to the outward 
	 * edges and their vertices.
	 * @return iterator to the outward edges and their destination vertices
	 */
	public OutEdgeIterator<I, E> getOutEdgeIterator();
	
	/**
	 * Send a message to a vertex id.
	 * @param id vertex id to send the message to
	 * @param msg message data to send
	 */
	public void sendMsg(I id, M msg);
	
	/**
	 * Send a message to all edges.
	 */
	public void sentMsgToAllEdges(M msg);
	
	/**
	 * After this is called, the compute() code will no longer be called for
	 * this vertice unless a message is sent to it.  Then the compute() code
	 * will be called once again until this function is called.  The application
	 * finishes only when all vertices vote to halt.
	 */
	public void voteToHalt();
	
	/**
	 * Is this vertex done?
	 */
	public boolean isHalted();
}
