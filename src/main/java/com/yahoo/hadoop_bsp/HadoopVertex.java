package com.yahoo.hadoop_bsp;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class
    HadoopVertex<I extends WritableComparable, V, E, M extends Writable>
        implements MutableVertex<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(HadoopVertex.class);
    /** Class-wide superstep */
    private static long m_superstep = 0;
    /** Class-wide number of vertices */
    private static long m_numVertices = -1;
    /** BSP Mapper for this Vertex */
    private BspJob.BspMapper<I, V, E, M> m_bspMapper;
    /** Vertex id */
    private I m_vertexId;
    /** Vertex value */
    private V m_vertexValue;
    /** Map of destination vertices and their edge values */
    private Map<I, E> m_destEdgeMap = new TreeMap<I, E>();
    /** If true, do not do anymore computation on this vertex. */
    private boolean m_halt = false;

    public final void addEdge(I destVertexId, E edgeValue) {
        E value = m_destEdgeMap.get(destVertexId);
        if (value != null) {
            LOG.debug("addEdge: Vertex=" + m_vertexId + ": already added an edge " +
                     "value for destination vertex " + destVertexId);
        }
        m_destEdgeMap.put(destVertexId, edgeValue);
    }

    public final void setVertexId(I vertexId) {
        m_vertexId = vertexId;
    }

    public final I getVertexId() {
        return m_vertexId;
    }

    public final void setBspMapper(BspJob.BspMapper<I, V, E, M> bspMapper) {
        m_bspMapper = bspMapper;
    }

    public static void setSuperstep(long superstep) {
        m_superstep = superstep;
    }

    public final long getSuperstep() {
        return m_superstep;
    }
    public final V getVertexValue() {
        return m_vertexValue;
    }
    public final void setVertexValue(V vertexValue) {
        m_vertexValue = vertexValue;
    }

    public static void setNumVertices(long numVertices) {
        m_numVertices = numVertices;
    }

    public final long getNumVertices() {
        return m_numVertices;
    }

    public final long getNumEdges() {
        return m_destEdgeMap.size();
    }

    /**
     * Implements the {@link OutEdgeIterator} for {@link HadoopVertex}
     * @author aching
     *
     * @param <I>
     * @param <E>
     */
    public final static class HadoopVertexOutEdgeIterator<I, E>
        implements OutEdgeIterator<I, E> {
        /** Reference to the original map */
        private final Map<I, E> m_destEdgeMap;
        /** Set of map entries*/
        private Set<Map.Entry<I, E>> m_destEdgeMapSet;
        /** Map of the destination vertices and their edge values */
        private Iterator<Map.Entry<I, E>> m_destEdgeMapSetIt;

        public HadoopVertexOutEdgeIterator(Map<I, E> destEdgeMap) {
            m_destEdgeMap = destEdgeMap;
            m_destEdgeMapSet = m_destEdgeMap.entrySet();
            m_destEdgeMapSetIt = m_destEdgeMapSet.iterator();
        }

        public boolean hasNext() {
            return m_destEdgeMapSetIt.hasNext();
        }

        public Entry<I, E> next() {
            return m_destEdgeMapSetIt.next();
        }

        public void remove() {
            m_destEdgeMapSetIt.remove();
        }

        public long size() {
            return m_destEdgeMapSet.size();
        }
    }

    public final OutEdgeIterator<I, E> getOutEdgeIterator() {
        return new HadoopVertexOutEdgeIterator<I, E>(m_destEdgeMap);
    }

    public final void sendMsg(I id, M msg) {
       m_bspMapper.sendMsg(id, msg);
    }

    public final void sentMsgToAllEdges(M msg) {
      for (Entry<I, E> destEdge : m_destEdgeMap.entrySet()) {
          sendMsg(destEdge.getKey(), msg);
      }
    }

    public final void voteToHalt() {
        m_halt = true;
    }

    public final boolean isHalted() {
        return m_halt;
    }

    /**
     * Register an aggregator.
     *
     * @param name of aggregator
     * @param aggregator
     * @return boolean (false when already registered)
     */
    public final static <A extends Writable> boolean registerAggregator(
                                      String name, Aggregator<A> aggregator) {
       return BspJob.BspMapper.registerAggregator(name, aggregator);
    }

    /**
     * Get a registered aggregator.
     *
     * @param name of aggregator
     * @return Aggregator<A> (null when not registered)
     */
    public final static <A extends Writable> Aggregator<A> getAggregator(
                                      String name) {
       return BspJob.BspMapper.getAggregator(name);
    }

    /**
     * Use a registered aggregator in current superstep.
     * Even when the same aggregator should be used in the next
     * superstep, useAggregator needs to be called at the beginning
     * of that superstep.
     *
     * @param name of aggregator
     * @return boolean (false when not registered)
     */
    public final static boolean useAggregator(String name) {
       return BspJob.BspMapper.useAggregator(name);
    }

}

