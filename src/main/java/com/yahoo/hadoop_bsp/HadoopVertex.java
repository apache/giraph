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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public abstract class HadoopVertex<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        implements MutableVertex<I, V, E, M>, Writable, Configurable {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(HadoopVertex.class);
    /** Class-wide superstep */
    private static long m_superstep = 0;
    /** Class-wide number of vertices */
    private static long m_numVertices = -1;
    /** Class-wide map context */
    private static Context context = null;
    /** Instantiatable vertex reader */
    private VertexReader m_instantiableVertexReader;
    /** BSP Mapper for this Vertex */
    private BspJob.BspMapper<I, V, E, M> m_bspMapper;
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
    /** Configuration */
    private Configuration conf;

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

    public final void setBspMapper(BspJob.BspMapper<I, V, E, M> bspMapper) {
        m_bspMapper = bspMapper;
    }

    public static void setSuperstep(long superstep) {
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

    public static void setNumVertices(long numVertices) {
        m_numVertices = numVertices;
    }

    @Override
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
                    getConf().getClass(
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

    /**
     * Register an aggregator in preSuperstep() and/or preApplication().
     *
     * @param name of aggregator
     * @param aggregator
     * @return boolean (false when already registered)
     */
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return m_bspMapper.getAggregatorUsage().registerAggregator(
            name, aggregatorClass);
    }

    /**
     * Get a registered aggregator.
     *
     * @param name of aggregator
     * @return Aggregator<A> (null when not registered)
     */
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return m_bspMapper.getAggregatorUsage().getAggregator(name);
    }

    /**
     * Use a registered aggregator in current superstep.
     * Even when the same aggregator should be used in the next
     * superstep, useAggregator needs to be called at the beginning
     * of that superstep in preSuperstep().
     *
     * @param name of aggregator
     * @return boolean (false when not registered)
     */
    public final boolean useAggregator(String name) {
        return m_bspMapper.getAggregatorUsage().useAggregator(name);
    }

    public List<M> getMsgList() {
        return m_msgList;
    }

    public final Configuration getConf() {
        return conf;
    }

    public final void setConf(Configuration conf) {
        this.conf = conf;
    }

    public final Context getContext() {
        return context;
    }

    public static void setContext(Context context) {
        HadoopVertex.context = context;
    }
}

