package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Simple balancer that maintains the distribution of vertex ranges that are
 * created by the InputSplit processing.  Vertex ranges never change from the
 * initial configuration and are hence, static.
 */
@SuppressWarnings("rawtypes")
public final class StaticBalancer<I extends WritableComparable,
                                  V extends Writable,
                                  E extends Writable,
                                  M extends Writable>
        extends BspBalancer<I, V, E, M>
        implements VertexRangeBalancer<I, V, E, M> {

    public final void rebalance() {
        setNextVertexRangeMap(getPrevVertexRangeMap());
    }
}
