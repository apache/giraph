package org.apache.giraph;

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
                               extends Vertex<I, V, E, M>, Writable {
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
