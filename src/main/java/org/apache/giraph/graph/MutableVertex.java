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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Map;

/**
 * Interface used by VertexReader to set the properties of a new vertex
 * or mutate the graph.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public abstract class MutableVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BasicVertex<I, V, E, M> {
  /**
   * Set the vertex id
   *
   * @param id Vertex id is set to this (instantiated by the user)
   */
  public abstract void setVertexId(I id);

  /**
   * Add an edge for this vertex (happens immediately)
   *
   * @param targetVertexId target vertex
   * @param edgeValue value of the edge
   * @return Return true if succeeded, false otherwise
   */
  public abstract boolean addEdge(I targetVertexId, E edgeValue);

  /**
   * Removes an edge for this vertex (happens immediately).
   *
   * @param targetVertexId the target vertex id of the edge to be removed.
   * @return the value of the edge which was removed (or null if no
   *         edge existed to targetVertexId)
   */
  public abstract E removeEdge(I targetVertexId);

  /**
   * Create a vertex to add to the graph.  Calls initialize() for the vertex
   * as well.
   *
   * @param vertexId Id of the new vertex.
   * @param vertexValue Value of the new vertex.
   * @param edges Map of edges to be added to this vertex.
   * @param messages Messages to be added to the vertex (typically empty)
   * @return A new vertex for adding to the graph
   */
  public BasicVertex<I, V, E, M> instantiateVertex(
      I vertexId, V vertexValue, Map<I, E> edges, Iterable<M> messages) {
    MutableVertex<I, V, E, M> mutableVertex =
        (MutableVertex<I, V, E, M>) BspUtils
        .<I, V, E, M>createVertex(getContext().getConfiguration());
    mutableVertex.setGraphState(getGraphState());
    mutableVertex.initialize(vertexId, vertexValue, edges, messages);
    return mutableVertex;
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.  Use instantiateVertex() to do the instantiation.
   *
   * @param vertex User created vertex
   */
  public void addVertexRequest(BasicVertex<I, V, E, M> vertex)
    throws IOException {
    getGraphState().getWorkerCommunications().
    addVertexReq(vertex);
  }

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param vertexId Id of the vertex to be removed.
   */
  public void removeVertexRequest(I vertexId) throws IOException {
    getGraphState().getWorkerCommunications().
    removeVertexReq(vertexId);
  }

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next superstep)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   */
  public void addEdgeRequest(I sourceVertexId, Edge<I, E> edge)
    throws IOException {
    getGraphState().getWorkerCommunications().
    addEdgeReq(sourceVertexId, edge);
  }

  /**
   * Request to remove an edge of a vertex from the graph
   * (processed just prior to the next superstep).
   *
   * @param sourceVertexId Source vertex id of edge
   * @param destVertexId Destination vertex id of edge
   */
  public void removeEdgeRequest(I sourceVertexId, I destVertexId)
    throws IOException {
    getGraphState().getWorkerCommunications().
    removeEdgeReq(sourceVertexId, destVertexId);
  }
}
