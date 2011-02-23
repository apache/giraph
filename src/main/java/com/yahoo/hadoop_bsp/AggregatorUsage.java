package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;

/**
 * Vertex classes can use this interface to register and use aggregators
 *
 * @param <A> Aggregator type must extend Writable
 */
public interface AggregatorUsage {
    /**
     * Register an aggregator in preSuperstep() and/or preApplication().
     *
     * @param name of aggregator
     * @param aggregatorClass Class type of the aggregator
     * @return created Aggregator or null when already registered
     */
    public <A extends Writable> Aggregator<A> registerAggregator(
        String name,
        Class<? extends Aggregator<A>> aggregatorClass);

    /**
     * Get a registered aggregator.
     *
     * @param name of aggregator
     * @return Aggregator<A> (null when not registered)
     */
    public Aggregator<? extends Writable> getAggregator(String name);

    /**
     * Use a registered aggregator in current superstep.
     * Even when the same aggregator should be used in the next
     * superstep, useAggregator needs to be called at the beginning
     * of that superstep in preSuperstep().
     *
     * @param name of aggregator
     * @return boolean (false when not registered)
     */
    public boolean useAggregator(String name);
}
