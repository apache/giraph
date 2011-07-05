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
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.BasicVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Public interface for workers to do message communication
 *
 * @param <I extends Writable> vertex id
 * @param <V extends Writable> vertex value
 * @param <E extends Writable> edge value
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
public interface WorkerCommunications<I extends WritableComparable,
                                      V extends Writable,
                                      E extends Writable,
                                      M extends Writable> {
    /**
     * Clean the cached map of vertex addresses that have changed
     * because of rebalancing.
     */
    void cleanCachedVertexAddressMap();

    /**
     * Sends a message to destination vertex.
     *
     * @param id
     * @param msg
     */
    void sendMessageReq(I id, M msg);

    /**
     * Sends a list of vertices to the appropriate vertex range owner
     *
     * @param vertexRangeIndex vertex range that the vertices belong to
     * @param vertexList list of vertices assigned to the vertexRangeIndex
     */
    void sendVertexListReq(I vertexIndexMax,
                           List<BasicVertex<I, V, E, M>> vertexList);

    /**
     * Sends a request to the appropriate vertex range owner to add an edge
     *
     * @param vertexIndex Index of the vertex to get the request
     * @param edge Edge to be added
     * @throws IOException
     */
    void addEdgeReq(I vertexIndex, Edge<I, E> edge) throws IOException;

    /**
     * Sends a request to the appropriate vertex range owner to remove an edge
     *
     * @param vertexIndex Index of the vertex to get the request
     * @param destinationVertexIndex Index of the edge to be removed
     * @throws IOException
     */
    void removeEdgeReq(I vertexIndex, I destinationVertexIndex)
        throws IOException;

    /**
     * Sends a request to the appropriate vertex range owner to add a vertex
     *
     * @param vertex Vertex to be added
     * @throws IOException
     */
    void addVertexReq(MutableVertex<I, V, E, M> vertex) throws IOException;

    /**
     * Sends a request to the appropriate vertex range owner to remove a vertex
     *
     * @param vertexIndex Index of the vertex to be removed
     * @throws IOException
     */
    void removeVertexReq(I vertexIndex) throws IOException;

    /**
     * Get the vertices that were sent in the last iteration.  After getting
     * the map, the user should synchronize with it to insure it
     * is thread-safe.
     *
     * @return map of vertex ranges to vertices
     */
    Map<I, List<Vertex<I, V, E, M>>> getInVertexRangeMap();
}
