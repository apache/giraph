package com.yahoo.hadoop_bsp;

import java.util.Iterator;
import java.util.Map;

/**
 * Defines the interface for users to access the outgoing edges and their
 * destination vertices.
 * @author aching
 *
 * @param <I>
 * @param <E>
 */
public interface OutEdgeIterator<I, E> extends Iterator<Map.Entry<I, E>> {
    /**
     * Get the number of edges for this vertex
     * @return number of edges for this vertex
     */
    long size();
}
