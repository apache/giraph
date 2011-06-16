package com.yahoo.hadoop_bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * User applications should all subclass {@link HadoopVertex}.  Package access
 * should prevent users from accessing internal methods.
 *
 * @param <I> Vertex Index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class HadoopVertex<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        implements MutableVertex<I, V, E, M>, Writable {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(HadoopVertex.class);
    /** Class-wide superstep */
    private static long m_superstep = 0;
    /** Class-wide number of vertices */
    private static long m_numVertices = -1;
    /** Class-wide number of edges */
    private static long m_numEdges = -1;
    /** Class-wide map context */
    private static Mapper<?, ?, ?, ?>.Context context = null;
    /** Instantiable vertex reader */
    private VertexReader<I, V, E> m_instantiableVertexReader = null;
    /** Class-wide BSP Mapper for this Vertex */
    private BspJob.BspMapper<I, V, E, M> m_bspMapper = null;
    /** Vertex id */
    private I m_vertexId;
    /** Vertex value */
    private V m_vertexValue;
    /** Map of destination vertices and their edge values */
    private final Map<I, E> m_destEdgeMap = new TreeMap<I, E>();
    /** If true, do not do anymore computation on this vertex. */
    private boolean m_halt = false;
    /** List of incoming messages from the previous superstep */
    private final List<M> m_msgList = new ArrayList<M>();

    @Override
    public void preApplication()
            throws InstantiationException, IllegalAccessException {
        // Do nothing, might be overrided by the user
    }

    @Override
    public void postApplication() {
        // Do nothing, might be overrided by the user
    }

    @Override
    public void preSuperstep() {
        // Do nothing, might be overrided by the user
    }

    @Override
    public void postSuperstep() {
        // Do nothing, might be overrided by the user
    }

    @Override
    public final void addEdge(I destVertexId, E edgeValue) {
        E value = m_destEdgeMap.get(destVertexId);
        if (value != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("addEdge: Vertex=" + m_vertexId +
                          ": already added an edge " +
                          "value for destination vertex " + destVertexId);
            }
        }
        m_destEdgeMap.put(destVertexId, edgeValue);
    }

    @Override
    public final void setVertexId(I vertexId) {
        m_vertexId = vertexId;
    }

    @Override
    public final I getVertexId() {
        return m_vertexId;
    }

    /**
     * Set the BspMapper for this vertex (internal use).
     *
     * @param bspMapper Mapper to use for communication
     */
    final void setBspMapper(BspMapper<I, V, E ,M> bspMapper) {
        m_bspMapper = bspMapper;
    }

    /**
     * Set the global superstep for all the vertices (internal use)
     *
     * @param superstep New superstep
     */
    static void setSuperstep(long superstep) {
        m_superstep = superstep;
    }

    @Override
    public final long getSuperstep() {
        return m_superstep;
    }

    @Override
    public final V getVertexValue() {
        return m_vertexValue;
    }

    @Override
    public final void setVertexValue(V vertexValue) {
        m_vertexValue = vertexValue;
    }

    /**
     * Set the total number of vertices from the last superstep.
     *
     * @param numVertices Aggregate vertices in the last superstep
     */
    static void setNumVertices(long numVertices) {
        m_numVertices = numVertices;
    }

    @Override
    public final long getNumVertices() {
        return m_numVertices;
    }

    /**
     * Set the total number of edges from the last superstep.
     *
     * @param numEdges Aggregate edges in the last superstep
     */
    static void setNumEdges(long numEdges) {
        m_numEdges = numEdges;
    }

    @Override
    public final long getNumEdges() {
        return m_numEdges;
    }

    /**
     * Implements the {@link OutEdgeIterator} for {@link HadoopVertex}
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

    @Override
    public final OutEdgeIterator<I, E> getOutEdgeIterator() {
        return new HadoopVertexOutEdgeIterator<I, E>(m_destEdgeMap);
    }

    @Override
    public final void sendMsg(I id, M msg) {
       m_bspMapper.getWorkerCommunications().sendMessage(id, msg);
    }

    @Override
    public final void sentMsgToAllEdges(M msg) {
        for (Entry<I, E> destEdge : m_destEdgeMap.entrySet()) {
            sendMsg(destEdge.getKey(), msg);
        }
    }

    @Override
    public final void voteToHalt() {
        m_halt = true;
    }

    @Override
    public final boolean isHalted() {
        return m_halt;
    }

    @SuppressWarnings("unchecked")
    final public void readFields(DataInput in) throws IOException {
        if (m_instantiableVertexReader == null) {
            Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
                (Class<? extends VertexInputFormat<I, V, E>>)
                    getContext().getConfiguration().getClass(
                        BspJob.VERTEX_INPUT_FORMAT_CLASS,
                        VertexInputFormat.class,
                        VertexInputFormat.class);
            try {
                m_instantiableVertexReader =
                    vertexInputFormatClass.newInstance().createVertexReader(
                        null, null);
            } catch (Exception e) {
                throw new RuntimeException(
                    "readFields: Couldn't instantiate vertex reader");
            }
        }

        m_vertexId = (I) m_instantiableVertexReader.createVertexId();
        m_vertexId.readFields(in);
        m_vertexValue = (V) m_instantiableVertexReader.createVertexValue();
        m_vertexValue.readFields(in);
        long edgeMapSize = in.readLong();
        for (long i = 0; i < edgeMapSize; ++i) {
            I destVertexId = (I) m_instantiableVertexReader.createVertexId();
            destVertexId.readFields(in);
            E edgeValue = (E) m_instantiableVertexReader.createEdgeValue();
            edgeValue.readFields(in);
            addEdge(destVertexId, edgeValue);
        }
        long msgListSize = in.readLong();
        for (long i = 0; i < msgListSize; ++i) {
            M msg = createMsgValue();
            msg.readFields(in);
            m_msgList.add(msg);
        }
        m_halt = in.readBoolean();
    }

    @Override
    final public void write(DataOutput out) throws IOException {
        m_vertexId.write(out);
        m_vertexValue.write(out);
        out.writeLong(m_destEdgeMap.size());
        for (Entry<I, E> entry : m_destEdgeMap.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeLong(m_msgList.size());
        for (M msg : m_msgList) {
            msg.write(out);
        }
        out.writeBoolean(m_halt);
    }

    @Override
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return m_bspMapper.getAggregatorUsage().registerAggregator(
            name, aggregatorClass);
    }

    @Override
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return m_bspMapper.getAggregatorUsage().getAggregator(name);
    }

    @Override
    public final boolean useAggregator(String name) {
        return m_bspMapper.getAggregatorUsage().useAggregator(name);
    }

    @Override
    public List<M> getMsgList() {
        return m_msgList;
    }

    public final Mapper<?, ?, ?, ?>.Context getContext() {
        return context;
    }

    static void setContext(Mapper<?, ?, ?, ?>.Context context) {
        HadoopVertex.context = context;
    }
}

