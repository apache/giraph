package com.yahoo.hadoop_bsp;

import java.util.List;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is the basic implementation for all of the {@link VertexRangeBalancer}
 * methods except the rebalance() method.  That is up to the user to implement.
 * @author aching
 *
 * @param <I> vertex id
 */
@SuppressWarnings("rawtypes")
public abstract class BspBalancer<I extends WritableComparable>
    implements VertexRangeBalancer<I> {
    /** list of last vertex ranges, in order */
    private List<VertexRange<I>> m_lastVertexRangeList = null;
    /** list of next vertex ranges, in order */
    private List<VertexRange<I>> m_nextVertexRangeList = null;
    /** list of available workers (hostname + partition id) */
    private List<String> m_hostnameIdList = null;

    final public void setLastVertexRangeList(
        final List<VertexRange<I>> lastVertexRangeSet) {
        m_lastVertexRangeList = lastVertexRangeSet;
    }

    final public List<VertexRange<I>> getLastVertexRangeList() {
        return m_lastVertexRangeList;
    }

    final public void setNextVertexRangeList(
        List<VertexRange<I>> vertexRangeList) {
        m_nextVertexRangeList = vertexRangeList;
    }

    final public List<VertexRange<I>> getNextVertexRangeList() {
        return m_nextVertexRangeList;
    }

    final public void setHostnameIdList(List<String> hostnameIdList) {
        m_hostnameIdList = hostnameIdList;
    }

    final public List<String> getHostnameIdList() {
        return m_hostnameIdList;
    }
}
