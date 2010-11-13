package com.yahoo.hadoop_bsp;

import java.util.ArrayList;

/**
 * Internal data structure used by BSPJob to store the actual user-defined
 * vertex data and edges.
 * @author aching
 *
 */
public class VertexData<V, E> {
	/** Edges are stored as a list, since they do not need random access */
	private ArrayList<E> m_edgeList;
	/** User data associated with a vertex (likely not edges) */
	private V m_userData;
	/** Done? */
	private boolean m_done = false;
	
	public void setEdgeList(ArrayList<E> edgeList) {
		m_edgeList = edgeList;
	}
	public ArrayList<E> getEdgeList() {
		return m_edgeList;
	}
	public void setUserData(V userData) {
		m_userData = userData;
	}
	public V getUserData() {
		return m_userData;
	}
	public void setDone(boolean done) {
		m_done = done;
	}
	public boolean isDone() {
		return m_done;
	}
}
