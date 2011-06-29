package org.apache.giraph.graph;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;

/**
 * Interface to determine your own balancing of vertex ranges
 * among the workers.
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
@SuppressWarnings("rawtypes")
public interface BasicVertexRangeBalancer<I extends WritableComparable,
                                          V extends Writable,
                                          E extends Writable,
                                          M extends Writable> {
    /**
     * Get the upcoming superstep number (since this happens prior to the
     * computation of the superstep
     *
     * @return the upcoming superstep
     */
    long getSuperstep();

    /**
     * Get the last determined VertexRanges for the index type for the previous
     * superstep.  If this is the first superstep, then it was last determined
     * by the loading.
     *
     * @return map containing last superstep's vertex ranges determination
     */
    NavigableMap<I, VertexRange<I, V, E, M>> getPrevVertexRangeMap();

    /**
     * Get a list of available workers and associated hostname and port
     * information.  This list can be used to assign the
     * vertices in rebalance().
     *
     * @return
     */
    Map<String, JSONArray> getWorkerHostnamePortMap();

    /**
     * User needs to implement this function and return the new vertex range
     * assignments.
     *
     * @return Map containing current superstep's vertex ranges determination
     */
    NavigableMap<I, VertexRange<I, V, E, M>> rebalance();
}
