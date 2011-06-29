package org.apache.giraph.graph;

import java.util.NavigableMap;

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
        extends VertexRangeBalancer<I, V, E, M> {

    @Override
    public final NavigableMap<I, VertexRange<I, V, E, M>> rebalance() {
        return getPrevVertexRangeMap();
    }
}
