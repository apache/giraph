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
	ArrayList<E> m_edgeList;
	/** User data associated with a vertex (likely not edges) */
	V m_userData;
}
