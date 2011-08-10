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

package org.apache.giraph.graph;

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
    private final List<BasicVertex<I, V, E, M>> addedVertexList =
        new ArrayList<BasicVertex<I, V, E, M>>();
    /** Count of remove vertex requests */
    private int removedVertexCount = 0;
    /** List of added edges */
    private final List<Edge<I, E>> addedEdgeList = new ArrayList<Edge<I, E>>();
    /** List of removed edges */
    private final List<I> removedEdgeList = new ArrayList<I>();

    @Override
    public List<BasicVertex<I, V, E, M>> getAddedVertexList() {
        return addedVertexList;
    }

    /**
     * Add a vertex mutation
     *
     * @param vertex Vertex to be added
     */
    public void addVertex(BasicVertex<I, V, E, M> vertex) {
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
