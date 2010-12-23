package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.WritableComparable;

/**
 * Interface used by VertexReader to set the properties of a new vertex.
 * @author aching
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M>
 */
public interface MutableVertex<I extends WritableComparable, V, E, M>
    extends Vertex<I, V, E, M> {
    /**
     * Set the vertex id
     * @param id vertex id is set to this (instantiated by the user)
     */
    public void setVertexId(I id);

    /**
     * Add an edge for this vertex
     * @param destVertexId destination vertex
     * @param edgeValue edge value
     */
    public void addEdge(I destVertexId, E edgeValue);
}
