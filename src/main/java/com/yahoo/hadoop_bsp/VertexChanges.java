package com.yahoo.hadoop_bsp;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Structure to hold all the possible graph mutations that can occur during a
 * superstep.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public interface VertexChanges<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable> {

    /**
     * Get the added vertices for this particular vertex index from the previous
     * superstep.
     *
     * @return List of vertices for this vertex index.
     */
    List<Vertex<I, V, E, M>> getAddedVertexList();

    /**
     * Get the number of times this vertex was removed in the previous
     * superstep.
     *
     * @return Count of time this vertex was removed in the previous superstep
     */
    int getRemovedVertexCount();

    /**
     * Get the added edges for this particular vertex index from the previous
     * superstep
     *
     * @return List of added edges for this vertex index
     */
    List<Edge<I, E>> getAddedEdgeList();

    /**
     * Get the removed edges by their destination vertex index.
     *
     * @return List of destination edges for removal from this vertex index
     */
    List<I> getRemovedEdgeList();
}
