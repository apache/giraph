package com.yahoo.hadoop_bsp;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public abstract class HadoopVertex<I, V, E, M> implements Vertex<I, V, E, M> {
	private static long m_superstep = 0;
	private static long m_numVertices = -1;
	private BspJob.BspMapper<I, V, E, M> m_bspMapper;
	private I m_id;
	private V m_vertexValue;
	private Map<I, E> m_destEdgeMap = new TreeMap<I, E>();
	private boolean m_halt = false;
	
	public final void addEdge(I destVertexId, E edgeValue) {
	    E value = m_destEdgeMap.get(destVertexId);
	    if (value != null) {
	        throw new RuntimeException("addEdge: Already added an edge " + 
	                                   "value for vertex " + destVertexId);
	    }
		m_destEdgeMap.put(destVertexId, edgeValue);
	}
	
	public final void setId(I id) {
		m_id = id;
	}
	
	public final I id() {
		return m_id;
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
	
	public long getNumVertices() {
	    return m_numVertices;
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
	    /** */
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
        OutEdgeIterator<I, E> destEdgeIt = getOutEdgeIterator();
        while (destEdgeIt.hasNext()) {
            Entry<I, E> destEdge = destEdgeIt.next();
            sendMsg(destEdge.getKey(), msg);
        }
    }
	
	public final void voteToHalt() {
		m_halt = true;
	}
	
	public final boolean isHalted() {
		return m_halt;
	}
}

