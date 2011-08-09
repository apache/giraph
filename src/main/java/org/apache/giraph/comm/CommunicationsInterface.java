/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
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

package org.apache.giraph.comm;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.VertexRange;
/*if[HADOOP_NON_SECURE]
 else[HADOOP_NON_SECURE]*/
import org.apache.giraph.hadoop.BspTokenSelector;
import org.apache.hadoop.security.token.TokenInfo;
/*end[HADOOP_NON_SECURE]*/
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Basic interface for communication between workers.
 *
 *
 * @param <I extends Writable> vertex id
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
/*if[HADOOP_NON_SECURE]
 else[HADOOP_NON_SECURE]*/
@TokenInfo(BspTokenSelector.class)
/*end[HADOOP_NON_SECURE]*/
public interface CommunicationsInterface<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VersionedProtocol {

    /**
     * Interface Version History
     *
     * 0 - First Version
     */
    static final long versionID = 0L;

    /**
     * Adds incoming message.
     *
     * @param vertexIndex
     * @param msg
     * @throws IOException
     */
    void putMsg(I vertexIndex, M msg) throws IOException;

    /**
     * Adds incoming message list.
     *
     * @param vertexIndex Vertex index where the message are added
     * @param msgList messages added
     * @throws IOException
     */
    void putMsgList(I vertexIndex, MsgList<M> msgList) throws IOException;

    /**
     * Adds vertex list (index, value, edges, etc.) to the appropriate worker.
     *
     * @param vertexIndexMax Max vertex index of {@link VertexRange}
     * @param vertexList List of vertices to add
     */
    void putVertexList(I vertexIndexMax,
                       VertexList<I, V, E, M> vertexList)
        throws IOException;

    /**
     * Add an edge to a remote vertex
     *
     * @param vertexIndex Vertex index where the edge is added
     * @param edge Edge to be added
     * @throws IOException
     */
    void addEdge(I vertexIndex, Edge<I, E> edge) throws IOException;

    /**
     * Remove an edge on a remote vertex
     *
     * @param vertexIndex Vertex index where the edge is added
     * @param destinationVertexIndex Edge vertex index to be removed
     * @throws IOException
     */
    void removeEdge(I vertexIndex, I destinationVertexIndex) throws IOException;

    /**
     * Add a remote vertex
     *
     * @param vertex Vertex that will be added
     * @throws IOException
     */
    void addVertex(MutableVertex<I, V, E, M> vertex) throws IOException;

    /**
     * Removed a remote vertex
     *
     * @param vertexIndex Vertex index representing vertex to be removed
     * @throws IOException
     */
    void removeVertex(I vertexIndex) throws IOException;

    /**
     * @return The name of this worker in the format "hostname:port".
     */
    String getName();
}
