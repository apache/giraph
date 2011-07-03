/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private static long superstep = 0;
    /** Class-wide number of vertices */
    private static long numVertices = -1;
    /** Class-wide number of edges */
    private static long numEdges = -1;
    /** Class-wide map context */
    private static Mapper.Context context = null;
    /** Class-wide BSP Mapper for this Vertex */
    private static GiraphJob.BspMapper<?, ? ,?, ?> bspMapper = null;
    /** Vertex id */
    private I vertexId = null;
    /** Vertex value */
    private V vertexValue = null;
    /** Map of destination vertices and their edge values */
    private final SortedMap<I, Edge<I, E>> destEdgeMap =
        new TreeMap<I, Edge<I, E>>();
    /** If true, do not do anymore computation on this vertex. */
    private boolean halt = false;
    /** List of incoming messages from the previous superstep */
    private final List<M> msgList = new ArrayList<M>();

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
        if (destEdgeMap.put(edge.getDestinationVertexIndex(), edge) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("addEdge: Vertex=" + vertexId +
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
        this.vertexId = vertexId;
    }

    @Override
    public final I getVertexId() {
        return vertexId;
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
        Vertex.bspMapper = bspMapper;
    }

    /**
     * Set the global superstep for all the vertices (internal use)
     *
     * @param superstep New superstep
     */
    static void setSuperstep(long superstep) {
        Vertex.superstep = superstep;
    }

    @Override
    public final long getSuperstep() {
        return superstep;
    }

    @Override
    public final V getVertexValue() {
        return vertexValue;
    }

    @Override
    public final void setVertexValue(V vertexValue) {
        this.vertexValue = vertexValue;
    }

    /**
     * Set the total number of vertices from the last superstep.
     *
     * @param numVertices Aggregate vertices in the last superstep
     */
    static void setNumVertices(long numVertices) {
        Vertex.numVertices = numVertices;
    }

    @Override
    public final long getNumVertices() {
        return numVertices;
    }

    /**
     * Set the total number of edges from the last superstep.
     *
     * @param numEdges Aggregate edges in the last superstep
     */
    static void setNumEdges(long numEdges) {
        Vertex.numEdges = numEdges;
    }

    @Override
    public final long getNumEdges() {
        return numEdges;
    }

    @Override
    public final SortedMap<I, Edge<I, E>> getOutEdgeMap() {
        return destEdgeMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void sendMsg(I id, M msg) {
        ((BspMapper<I, V, E, M>) bspMapper).
            getWorkerCommunications().sendMessageReq(id, msg);
    }

    @Override
    public final void sentMsgToAllEdges(M msg) {
        for (Edge<I, E> edge : destEdgeMap.values()) {
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
        ((BspMapper<I, V, E, M>) bspMapper).
            getWorkerCommunications().addVertexReq(vertex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeVertexRequest(I vertexId) throws IOException {
        ((BspMapper<I, V, E, M>) bspMapper).
            getWorkerCommunications().removeVertexReq(vertexId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addEdgeRequest(I vertexIndex,
                               Edge<I, E> edge) throws IOException {
        ((BspMapper<I, V, E, M>) bspMapper).
            getWorkerCommunications().addEdgeReq(vertexIndex, edge);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeEdgeRequest(I sourceVertexId,
                                  I destVertexId) throws IOException {
        ((BspMapper<I, V, E, M>) bspMapper).
            getWorkerCommunications().removeEdgeReq(sourceVertexId,
                                                    destVertexId);
    }

    @Override
    public final void voteToHalt() {
        halt = true;
    }

    @Override
    public final boolean isHalted() {
        return halt;
    }

    @Override
    final public void readFields(DataInput in) throws IOException {
        vertexId =
            BspUtils.<I>createVertexIndex(getContext().getConfiguration());
        vertexId.readFields(in);
        boolean hasVertexValue = in.readBoolean();
        if (hasVertexValue) {
            vertexValue =
                BspUtils.<V>createVertexValue(getContext().getConfiguration());
            vertexValue.readFields(in);
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
            msgList.add(msg);
        }
        halt = in.readBoolean();
    }

    @Override
    final public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        out.writeBoolean(vertexValue != null);
        if (vertexValue != null) {
            vertexValue.write(out);
        }
        out.writeLong(destEdgeMap.size());
        for (Edge<I, E> edge : destEdgeMap.values()) {
            edge.write(out);
        }
        out.writeLong(msgList.size());
        for (M msg : msgList) {
            msg.write(out);
        }
        out.writeBoolean(halt);
    }

    @Override
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return bspMapper.getAggregatorUsage().registerAggregator(
            name, aggregatorClass);
    }

    @Override
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return bspMapper.getAggregatorUsage().getAggregator(name);
    }

    @Override
    public final boolean useAggregator(String name) {
        return bspMapper.getAggregatorUsage().useAggregator(name);
    }

    @Override
    public List<M> getMsgList() {
        return msgList;
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

