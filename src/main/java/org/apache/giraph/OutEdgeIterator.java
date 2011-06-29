package org.apache.giraph;

import java.util.Iterator;
import java.util.Map;

/**
 * Defines the interface for users to access the outgoing edges and their
 * destination vertices.
 *
 * @param <I> Vertex index value
 * @param <E> Edge value
 */
public interface OutEdgeIterator<I, E> extends Iterator<Map.Entry<I, E>> {
    /**
     * Get the number of edges for this vertex
     *
     * @return Number of edges for this vertex
     */
    long size();
}
