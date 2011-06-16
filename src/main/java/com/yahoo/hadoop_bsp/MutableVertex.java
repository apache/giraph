package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface used by VertexReader to set the properties of a new vertex.
 */
@SuppressWarnings("rawtypes")
public interface MutableVertex<I extends WritableComparable,
                               V extends Writable,
                               E extends Writable,
                               M extends Writable>
                               extends Vertex<I, V, E, M> {
    /**
     * Set the vertex id
     * @param id vertex id is set to this (instantiated by the user)
     */
    void setVertexId(I id);

    /**
     * Add an edge for this vertex
     * @param destVertexId destination vertex
     * @param edgeValue edge value
     */
    void addEdge(I destVertexId, E edgeValue);
}
