/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User applications often subclass {@link Vertex}, which stores the outbound
 * edges in SortedMap, for both random-access and range operations.
 * User applications which need to implement their own
 * in-memory data structures should subclass {@link MutableVertex}.
 *
 * Package access will prevent users from accessing internal methods.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<I extends WritableComparable, V extends Writable,
        E extends Writable, M extends Writable>
        extends MutableVertex<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(Vertex.class);
    /** Vertex id */
    private I vertexId = null;
    /** Vertex value */
    private V vertexValue = null;
    /** Map of destination vertices and their edge values */
    protected final Map<I, Edge<I, E>> destEdgeMap =
        new HashMap<I, Edge<I, E>>();
    /** List of incoming messages from the previous superstep */
    private final List<M> msgList = Lists.newArrayList();

    @Override
    public void initialize(I vertexId, V vertexValue, Map<I, E> edges, List<M> messages) {
        if (vertexId != null) {
            setVertexId(vertexId);
        }
        if (vertexValue != null) {
            setVertexValue(vertexValue);
        }
        if (edges != null && !edges.isEmpty()) {
            for (Map.Entry<I, E> entry : edges.entrySet()) {
                destEdgeMap.put(
                    entry.getKey(),
                    new Edge<I, E>(entry.getKey(), entry.getValue()));
            }
        }
        if (messages != null && !messages.isEmpty()) {
            msgList.addAll(messages);
        }
    }

    @Override
    public final boolean addEdge(I targetVertexId, E edgeValue) {
        if (destEdgeMap.put(
                targetVertexId,
                new Edge<I, E>(targetVertexId, edgeValue)) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("addEdge: Vertex=" + vertexId +
                          ": already added an edge value for dest vertex id " +
                          targetVertexId);
            }
            return false;
        } else {
            return true;
        }
    }

    @Override
    public long getSuperstep() {
        return getGraphState().getSuperstep();
    }

    @Override
    public final void setVertexId(I vertexId) {
        this.vertexId = vertexId;
    }

    @Override
    public final I getVertexId() {
        return vertexId;
    }

    @Override
    public final V getVertexValue() {
        return vertexValue;
    }

    @Override
    public final void setVertexValue(V vertexValue) {
        this.vertexValue = vertexValue;
    }

    @Override
    public E getEdgeValue(I targetVertexId) {
        Edge<I, E> edge = destEdgeMap.get(targetVertexId);
        return edge != null ? edge.getEdgeValue() : null;
    }

    @Override
    public boolean hasEdge(I targetVertexId) {
        return destEdgeMap.containsKey(targetVertexId);
    }

    /**
     * Get an iterator to the edges on this vertex.
     *
     * @return A <em>sorted</em> iterator, as defined by the sort-order
     *         of the vertex ids
     */
    @Override
    public Iterator<I> iterator() {
        return destEdgeMap.keySet().iterator();
    }

    @Override
    public int getNumOutEdges() {
        return destEdgeMap.size();
    }

    @Override
    public E removeEdge(I targetVertexId) {
        Edge<I, E> edge = destEdgeMap.remove(targetVertexId);
        if (edge != null) {
            return edge.getEdgeValue();
        } else {
            return null;
        }
    }

    @Override
    public final void sendMsgToAllEdges(M msg) {
        if (msg == null) {
            throw new IllegalArgumentException(
                "sendMsgToAllEdges: Cannot send null message to all edges");
        }
        for (Edge<I, E> edge : destEdgeMap.values()) {
            sendMsg(edge.getDestVertexId(), msg);
        }
    }


    @Override
    public void addVertexRequest(MutableVertex<I, V, E, M> vertex)
            throws IOException {
        getGraphState().getWorkerCommunications().
            addVertexReq(vertex);
    }

    @Override
    public void removeVertexRequest(I vertexId) throws IOException {
        getGraphState().getWorkerCommunications().
            removeVertexReq(vertexId);
    }

    @Override
    public void addEdgeRequest(I vertexIndex,
                               Edge<I, E> edge) throws IOException {
        getGraphState().getWorkerCommunications().
            addEdgeReq(vertexIndex, edge);
    }

    @Override
    public void removeEdgeRequest(I sourceVertexId,
                                  I destVertexId) throws IOException {
        getGraphState().getWorkerCommunications().
            removeEdgeReq(sourceVertexId, destVertexId);
    }

    @Override
    final public void readFields(DataInput in) throws IOException {
        vertexId = BspUtils.<I>createVertexIndex(getConf());
        vertexId.readFields(in);
        boolean hasVertexValue = in.readBoolean();
        if (hasVertexValue) {
            vertexValue = BspUtils.<V>createVertexValue(getConf());
            vertexValue.readFields(in);
        }
        long edgeMapSize = in.readLong();
        for (long i = 0; i < edgeMapSize; ++i) {
            Edge<I, E> edge = new Edge<I, E>();
            edge.setConf(getConf());
            edge.readFields(in);
            addEdge(edge.getDestVertexId(), edge.getEdgeValue());
        }
        long msgListSize = in.readLong();
        for (long i = 0; i < msgListSize; ++i) {
            M msg = BspUtils.<M>createMessageValue(getConf());
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
    void setMessages(Iterable<M> messages) {
        msgList.clear();
        for (M message : messages) {
            msgList.add(message);
        }
    }

    @Override
    public Iterable<M> getMessages() {
        return Iterables.unmodifiableIterable(msgList);
    }

    @Override
    void releaseResources() {
        // Hint to GC to free the messages
        msgList.clear();
    }

    @Override
    public String toString() {
        return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() +
            ",#edges=" + destEdgeMap.size() + ")";
    }
}

