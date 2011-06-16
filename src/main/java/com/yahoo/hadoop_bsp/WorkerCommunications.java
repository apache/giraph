package com.yahoo.hadoop_bsp;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Public interface for workers to do message communication
 *
 * @param <I extends Writable> vertex id
 * @param <V extends Writable> vertex value
 * @param <E extends Writable> edge value
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
public interface WorkerCommunications<I extends WritableComparable,
                                      V extends Writable,
                                      E extends Writable,
                                      M extends Writable> {
    /**
     * Clean the cached map of vertex addresses that have changed
     * because of rebalancing.
     */
    void cleanCachedVertexAddressMap();

    /**
     * Sends a message to destination vertex.
     *
     * @param id
     * @param msg
     */
    void sendMessage(I id, M msg);

    /**
     * Sends a list of vertices to the appropriate vertex range owner
     *
     * @param vertexRangeIndex vertex range that the vertices belong to
     * @param vertexList list of vertices assigned to the vertexRangeIndex
     */
    void sendVertexList(I vertexIndexMax,
                        List<Vertex<I, V, E, M>> vertexList);

    /**
     * Get the vertices that were sent in the last iteration.  After getting
     * the map, the user should synchronize with it to insure its thread-safe.
     *
     * @return map of vertex ranges to vertices
     */
    Map<I, List<HadoopVertex<I, V, E, M>>> getInVertexRangeMap();
}
