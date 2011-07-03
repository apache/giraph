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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface used by VertexReader to set the properties of a new vertex
 * or mutate the graph.
 */
@SuppressWarnings("rawtypes")
public interface MutableVertex<I extends WritableComparable,
                               V extends Writable,
                               E extends Writable,
                               M extends Writable>
                               extends BasicVertex<I, V, E, M>, Writable {
    /**
     * Set the vertex id
     *
     * @param id Vertex id is set to this (instantiated by the user)
     */
    void setVertexId(I id);

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to be added
     * @return Return true if succeeded, false otherwise
     */
    boolean addEdge(Edge<I, E> edge);

    /**
     * Create a vertex for use in addVertexRequest().  Still need to get the
     * vertex id and vertex value.
     *
     * @return Created vertex for addVertexRequest.
     */
    MutableVertex<I, V, E, M> instantiateVertex();

    /**
     * Sends a request to create a vertex that will be available during the
     * next superstep.  Use instantiateVertex() to do the instantiation.
     *
     * @return User created vertex
     */
    void addVertexRequest(MutableVertex<I, V, E, M> vertex) throws IOException;

    /**
     * Request to remove a vertex from the graph
     * (applied just prior to the next superstep).
     *
     * @param vertexId Id of the vertex to be removed.
     */
    void removeVertexRequest(I vertexId) throws IOException;

    /**
     * Request to add an edge of a vertex in the graph
     * (processed just prior to the next superstep)
     *
     * @param sourceVertexId Source vertex id of edge
     * @param edge Edge to add
     */
    void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) throws IOException;

    /**
     * Request to remove an edge of a vertex from the graph
     * (processed just prior to the next superstep).
     *
     * @param sourceVertexId Source vertex id of edge
     * @param destVertexId Destination vertex id of edge
     */
    void removeEdgeRequest(I sourceVertexId, I destVertexId) throws IOException;
}
