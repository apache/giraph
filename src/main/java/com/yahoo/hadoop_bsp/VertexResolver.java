package com.yahoo.hadoop_bsp;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Handles all the situations that can arise upon creation/removal of
 * vertices and edges.
 */
@SuppressWarnings("rawtypes")
public interface VertexResolver<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable> {
    /**
     * A vertex may have been removed, created zero or more times and had
     * zero or more messages sent to it.  This method will handle all situations
     * excluding the normal case (a vertex already exists and has zero or more
     * messages sent it to).
     *
     * @param originalVertex Original vertex or null if none
     * @param vertexChanges Changes that happened to this vertex or null if none
     * @param msgList List of messages received in the last superstep or null
     *        if none
     * @return Vertex to be returned, if null, and a vertex currently exists
     *         it will be removed
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    Vertex<I, V, E, M> resolve(Vertex<I, V, E, M> vertex,
                               VertexChanges<I, V, E, M> vertexChanges,
                               List<M> msgList);

    /**
     * Create a default vertex that can be used to return from resolve().
     *
     * @return Newly instantiated vertex.
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    Vertex<I, V, E, M> instantiateVertex();
}
