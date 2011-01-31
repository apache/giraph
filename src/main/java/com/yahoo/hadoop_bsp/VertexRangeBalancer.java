package com.yahoo.hadoop_bsp;

import java.util.List;

import org.apache.hadoop.io.WritableComparable;

/**
 * Implement this interface to determine your own balancing of vertex ranges
 * among the workers.
 * @author aching
 *
 * @param <I> index type
 */
@SuppressWarnings("rawtypes")
public interface VertexRangeBalancer<I extends WritableComparable> {
    /**
     * Do not override.  This will be used to set the last vertex range list
     * @param lastVertexRangeList list of vertex ranges from the last superstep
     */
    public void setLastVertexRangeList(
        final List<VertexRange<I>> lastVertexRangeList);

    /**
     * Get the last determined VertexRanges for the index type for the previous
     * superstep.  If this is the first superstep, then it was last determined
     * by the loading.
     *
     * @return list containing last superstep's vertex ranges determination
     */
    public List<VertexRange<I>> getLastVertexRangeList();

    /**
     * Set the vertex ranges in rebalance() for the upcoming superstep.
     */
    public void setNextVertexRangeList(
        List<VertexRange<I>> nextVertexRangeList);

    /**
     * Get the vertex range list set (must be called in rebalance() at least
     * once)
     *
     * @return list containing current superstep's vertex ranges determination
     */
    public List<VertexRange<I>> getNextVertexRangeList();

    /**
     * Do not override.  This will be used to set the available workers for the
     * rebalance() method.
     *
     * @param hostnameIdList list of available workers
     */
    public void setHostnameIdList(List<String> hostnameIdList);

    /**
     * Get a list of available workers.  This list can be used to assign the
     * vertices in rebalance().
     *
     * @return
     */
    public List<String> getHostnameIdList();

    /**
     * User needs to implement this function and ensure that setVertexRange
     * was called once.
     */
    public void rebalance();
}
