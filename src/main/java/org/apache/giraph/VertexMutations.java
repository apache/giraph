package org.apache.giraph;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Structure to hold all the possible graph mutations that can occur during a
 * superstep.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class VertexMutations<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable> implements VertexChanges<I, V, E, M> {
    /** List of added vertices during the last superstep */
    private final List<Vertex<I, V, E, M>> addedVertexList =
        new ArrayList<Vertex<I, V, E, M>>();
    /** Count of remove vertex requests */
    private int removedVertexCount = 0;
    /** List of added edges */
    private final List<Edge<I, E>> addedEdgeList = new ArrayList<Edge<I, E>>();
    /** List of removed edges */
    private final List<I> removedEdgeList = new ArrayList<I>();

    @Override
    public List<Vertex<I, V, E, M>> getAddedVertexList() {
        return addedVertexList;
    }

    /**
     * Add a vertex mutation
     *
     * @param vertex Vertex to be added
     */
    public void addVertex(Vertex<I, V, E, M> vertex) {
        addedVertexList.add(vertex);
    }

    @Override
    public int getRemovedVertexCount() {
        return removedVertexCount;
    }

    /**
     * Removed a vertex mutation (increments a count)
     */
    public void removeVertex() {
        ++removedVertexCount;
    }

    @Override
    public List<Edge<I, E>> getAddedEdgeList() {
        return addedEdgeList;
    }

    /**
     * Add an edge to this vertex
     *
     * @param edge Edge to be added
     */
    public void addEdge(Edge<I, E> edge) {
        addedEdgeList.add(edge);
    }

    @Override
    public List<I> getRemovedEdgeList() {
        return removedEdgeList;
    }

    /**
     * Remove an edge on this vertex
     *
     * @param destinationVertexId Vertex index of the destination of the edge
     */
    public void removeEdge(I destinationVertexId) {
        removedEdgeList.add(destinationVertexId);
    }

    @Override
    public String toString() {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("added vertices", getAddedVertexList().toString());
            jsonObject.put("added edges", getAddedEdgeList().toString());
            jsonObject.put("removed vertex count", getRemovedVertexCount());
            jsonObject.put("removed edges", getRemovedEdgeList().toString());
            return jsonObject.toString();
        } catch (JSONException e) {
            throw new IllegalStateException("toString: Got a JSON exception",
                                            e);
        }
    }
}
