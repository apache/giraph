package com.yahoo.hadoop_bsp;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;

/**
 * Implement this interface to determine your own balancing of vertex ranges
 * among the workers.
 * @author aching
 *
 * @param <I> index type
 */
@SuppressWarnings("rawtypes")
public interface VertexRangeBalancer<I extends WritableComparable,
                                     V extends Writable,
                                     E extends Writable,
                                     M extends Writable> {
    /**
     * Do not override.  This will be used to set the previous vertex range map
     * @param lastVertexRangeMap map of vertex ranges from the last superstep
     */
    void setPrevVertexRangeMap(
        final NavigableMap<I, VertexRange<I, V, E, M>> lastVertexRangeMap);

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
     * Set the vertex ranges in rebalance() for the upcoming superstep.
     */
    void setNextVertexRangeMap(
        NavigableMap<I, VertexRange<I, V, E, M>> nextVertexRangeMap);

    /**
     * Get the vertex range list set (must be called in rebalance() at least
     * once)
     *
     * @return list containing current superstep's vertex ranges determination
     */
    NavigableMap<I, VertexRange<I, V, E, M>> getNextVertexRangeMap();

    /**
     * Do not override.  This will be used to set the available workers and
     * associated hostname and port information for the
     * rebalance() method.
     *
     * @param hostnameIdList list of available workers
     */
    void setWorkerHostnamePortMap(Map<String, JSONArray> workerHostnamePortMap);

    /**
     * Do not override.  This will be used to set the previous hostname and
     * port information for the next vertex range list based on the last
     * vertex range list information.
     */
    void setPreviousHostnamePort();

    /**
     * Get a list of available workers and associated hostname and port
     * information.  This list can be used to assign the
     * vertices in rebalance().
     *
     * @return
     */
    Map<String, JSONArray> getWorkerHostnamePortMap();

    /**
     * User needs to implement this function and ensure that setVertexRange
     * was called once.
     */
    void rebalance();
}
