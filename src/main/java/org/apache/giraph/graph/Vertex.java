package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import org.apache.giraph.graph.GiraphJob.BspMapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * User applications should all subclass {@link Vertex}.  Package access
 * should prevent users from accessing internal methods.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        implements MutableVertex<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(Vertex.class);
    /** Class-wide superstep */
    private static long m_superstep = 0;
    /** Class-wide number of vertices */
    private static long m_numVertices = -1;
    /** Class-wide number of edges */
    private static long m_numEdges = -1;
    /** Class-wide map context */
    private static Mapper.Context context = null;
    /** Class-wide BSP Mapper for this Vertex */
    private static GiraphJob.BspMapper<?, ? ,?, ?> m_bspMapper = null;
    /** Vertex id */
    private I m_vertexId = null;
    /** Vertex value */
    private V m_vertexValue = null;
    /** Map of destination vertices and their edge values */
    private final SortedMap<I, Edge<I, E>> m_destEdgeMap =
        new TreeMap<I, Edge<I, E>>();
    /** If true, do not do anymore computation on this vertex. */
    private boolean m_halt = false;
    /** List of incoming messages from the previous superstep */
    private final List<M> m_msgList = new ArrayList<M>();

    @Override
    public void preApplication()
            throws InstantiationException, IllegalAccessException {
        // Do nothing, might be overriden by the user
    }

    @Override
    public void postApplication() {
        // Do nothing, might be overriden by the user
    }

    @Override
    public void preSuperstep() {
        // Do nothing, might be overriden by the user
    }

    @Override
    public void postSuperstep() {
        // Do nothing, might be overriden by the user
    }

    @Override
    public final boolean addEdge(Edge<I, E> edge) {
        edge.setConf(getContext().getConfiguration());
        if (m_destEdgeMap.put(edge.getDestinationVertexIndex(), edge) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("addEdge: Vertex=" + m_vertexId +
                          ": already added an edge " +
                          "value for destination vertex " +
                          edge.getDestinationVertexIndex());
            }
            return false;
        } else {
            return true;
        }
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
    final static <I extends WritableComparable,
            V extends Writable, E extends Writable,
            M extends Writable> void
            setBspMapper(BspMapper<I, V, E, M> bspMapper) {
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

    @Override
    public final SortedMap<I, Edge<I, E>> getOutEdgeMap() {
        return m_destEdgeMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void sendMsg(I id, M msg) {
        ((BspMapper<I, V, E, M>) m_bspMapper).
            getWorkerCommunications().sendMessageReq(id, msg);
    }

    @Override
    public final void sentMsgToAllEdges(M msg) {
        for (Edge<I, E> edge : m_destEdgeMap.values()) {
            sendMsg(edge.getDestinationVertexIndex(), msg);
        }
    }

    @Override
    public MutableVertex<I, V, E, M> instantiateVertex() {
        Vertex<I, V, E, M> mutableVertex =
            BspUtils.<I, V, E, M>createVertex(getContext().getConfiguration());
        return mutableVertex;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addVertexRequest(MutableVertex<I, V, E, M> vertex)
            throws IOException {
        ((BspMapper<I, V, E, M>) m_bspMapper).
            getWorkerCommunications().addVertexReq(vertex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeVertexRequest(I vertexId) throws IOException {
        ((BspMapper<I, V, E, M>) m_bspMapper).
            getWorkerCommunications().removeVertexReq(vertexId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addEdgeRequest(I vertexIndex,
                               Edge<I, E> edge) throws IOException {
        ((BspMapper<I, V, E, M>) m_bspMapper).
            getWorkerCommunications().addEdgeReq(vertexIndex, edge);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeEdgeRequest(I sourceVertexId,
                                  I destVertexId) throws IOException {
        ((BspMapper<I, V, E, M>) m_bspMapper).
            getWorkerCommunications().removeEdgeReq(sourceVertexId,
                                                    destVertexId);
    }

    @Override
    public final void voteToHalt() {
        m_halt = true;
    }

    @Override
    public final boolean isHalted() {
        return m_halt;
    }

    @Override
    final public void readFields(DataInput in) throws IOException {
        m_vertexId =
            BspUtils.<I>createVertexIndex(getContext().getConfiguration());
        m_vertexId.readFields(in);
        boolean hasVertexValue = in.readBoolean();
        if (hasVertexValue) {
            m_vertexValue =
                BspUtils.<V>createVertexValue(getContext().getConfiguration());
            m_vertexValue.readFields(in);
        }
        long edgeMapSize = in.readLong();
        for (long i = 0; i < edgeMapSize; ++i) {
            Edge<I, E> edge = new Edge<I, E>();
            edge.setConf(getContext().getConfiguration());
            edge.readFields(in);
            addEdge(edge);
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
        out.writeBoolean(m_vertexValue != null);
        if (m_vertexValue != null) {
            m_vertexValue.write(out);
        }
        out.writeLong(m_destEdgeMap.size());
        for (Edge<I, E> edge : m_destEdgeMap.values()) {
            edge.write(out);
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

    final static void setContext(Mapper<?, ?, ?, ?>.Context context) {
        Vertex.context = context;
    }

    @Override
    public String toString() {
        return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() +
            ",#edges=" + getOutEdgeMap().size() + ")";
    }
}

