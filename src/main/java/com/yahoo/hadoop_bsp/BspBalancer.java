package com.yahoo.hadoop_bsp;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.json.JSONArray;

/**
 * This is the basic implementation for all of the {@link VertexRangeBalancer}
 * methods except the rebalance() method.  That is up to the user to implement.
 * @author aching
 *
 * @param <I> vertex id
 */
@SuppressWarnings("rawtypes")
public abstract class BspBalancer<I extends WritableComparable,
                                  V extends Writable,
                                  E extends Writable,
                                  M extends Writable>
                                  implements VertexRangeBalancer<I, V, E, M> {
    /** map of prev vertex ranges, in order */
    private NavigableMap<I, VertexRange<I, V, E, M>>
        m_prevVertexRangeMap = null;
    /** map of next vertex ranges, in order */
    private NavigableMap<I, VertexRange<I, V, E, M>>
        m_nextVertexRangeMap = null;
    /** map of available workers to JSONArray(hostname + partition id) */
    private Map<String, JSONArray> m_workerHostnameIdMap = null;
    /** Current superstep */
    private long m_superstep;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspBalancer.class);

    final public long getSuperstep() {
        return m_superstep;
    }

    /**
     * Set the upcoming superstep number (Do not use, this is only meant for
     * the infrastructure)
     */
    final public void setSuperstep(long superstep) {
        m_superstep = superstep;
    }

    final public void setPrevVertexRangeMap(
        final NavigableMap<I, VertexRange<I, V, E, M>> prevVertexRangeMap) {
        m_prevVertexRangeMap = prevVertexRangeMap;
    }

    final public NavigableMap<I, VertexRange<I, V, E, M>>
            getPrevVertexRangeMap() {
        return m_prevVertexRangeMap;
    }

    final public void setNextVertexRangeMap(
            NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap) {
        m_nextVertexRangeMap = vertexRangeMap;
    }

    final public NavigableMap<I, VertexRange<I, V, E, M>>
            getNextVertexRangeMap() {
        return m_nextVertexRangeMap;
    }

    final public void setWorkerHostnamePortMap(
            Map<String, JSONArray> workerHostnamePortMap) {
        m_workerHostnameIdMap = workerHostnamePortMap;
    }

    final public void setPreviousHostnamePort() {
        if (getNextVertexRangeMap().size() !=
                getPrevVertexRangeMap().size()) {
            throw new RuntimeException(
                "setPreviousHostnamePort: Next vertex range set size " +
                getNextVertexRangeMap().size() +
                ", prev vertex range set size " +
                getPrevVertexRangeMap().size());
        }

        for (VertexRange<I, V, E, M> prevVertexRange :
                m_prevVertexRangeMap.values()) {
            if (!m_nextVertexRangeMap.containsKey(
                    prevVertexRange.getMaxIndex())) {
                throw new RuntimeException(
                    "setPreviousHostnamePort: Prev vertex range " +
                    prevVertexRange.getMaxIndex() +
                    " doesn't exist in new vertex range map.");
            }
            VertexRange<I, V, E, M> nextVertexRange =
                m_nextVertexRangeMap.get(prevVertexRange.getMaxIndex());
            nextVertexRange.setPreviousHostname(prevVertexRange.getHostname());
            nextVertexRange.setPreviousPort(prevVertexRange.getPort());
        }
    }

    final public long getVertexRangeChanges() {
        Set<String> hostnamePortSet = new HashSet<String>();
        for (VertexRange<I, V, E, M> vertexRange :
                getNextVertexRangeMap().values()) {
            if (!vertexRange.getHostname().equals(
                    vertexRange.getPreviousHostname()) ||
                    (vertexRange.getPort() != vertexRange.getPreviousPort())) {
                String hostnamePort = new String();
                hostnamePort += vertexRange.getHostname();
                hostnamePort += Integer.toString(vertexRange.getPort());
                hostnamePortSet.add(hostnamePort);
                String prevHostnamePort = new String();
                prevHostnamePort += vertexRange.getPreviousHostname();
                prevHostnamePort +=
                    Integer.toString(vertexRange.getPreviousPort());
                hostnamePortSet.add(prevHostnamePort);
            }
        }
        LOG.debug("getVertexRangeChanges: Waiting for " +
                  hostnamePortSet.size() + " workers");
        return hostnamePortSet.size();
    }

    final public Map<String, JSONArray> getWorkerHostnamePortMap() {
        return m_workerHostnameIdMap;
    }
}
