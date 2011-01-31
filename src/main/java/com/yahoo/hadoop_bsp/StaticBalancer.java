package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.WritableComparable;

/**
 * Simple balancer that maintains the distribution of vertex ranges that are
 * created by the InputSplit processing.  Vertex ranges never change from the
 * initial configuration and are hence, static.
 *
 * @author aching
 *
 * @param <I> vertex id type
 */
@SuppressWarnings("rawtypes")
public final class StaticBalancer<I extends WritableComparable>
    extends BspBalancer<I> implements VertexRangeBalancer<I> {

    public final void rebalance() {
        setNextVertexRangeList(getLastVertexRangeList());
    }
}
