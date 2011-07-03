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

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Default implementation of how to resolve vertex creation/removal, messages
 * to nonexistent vertices, etc.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M>
 */
@SuppressWarnings("rawtypes")
public class BspResolver<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        implements VertexResolver<I, V, E, M>, Configurable {
    /** Configuration */
    private Configuration conf = null;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspResolver.class);

    @Override
    public BasicVertex<I, V, E, M> resolve(
            BasicVertex<I, V, E, M> vertex,
            VertexChanges<I, V, E, M> vertexChanges,
            List<M> msgList) {
        // Default algorithm:
        // 1. If the vertex exists, first prune the edges
        // 2. If vertex removal desired, remove the vertex.
        // 3. If creation of vertex desired, pick first vertex
        // 4. If vertex doesn't exist, but got messages, create
        // 5. If edge addition, add the edges
        if (vertex != null) {
            if (vertexChanges != null) {
                List<I> removedEdgeList = vertexChanges.getRemovedEdgeList();
                for (I removedDestVertex : removedEdgeList) {
                    if (vertex.getOutEdgeMap().remove(removedDestVertex) ==
                            null) {
                        LOG.warn("resolve: Failed to remove edge with " +
                                 "destination " + removedDestVertex + "on " +
                                 vertex + " since it doesn't exist.");
                    }
                }
                if (vertexChanges.getRemovedVertexCount() > 0) {
                    vertex = null;
                }
            }
        }

        if (vertex == null) {
            if (vertexChanges != null) {
                if (!vertexChanges.getAddedVertexList().isEmpty()) {
                    vertex = vertexChanges.getAddedVertexList().get(0);
                }
            }
            if ((vertex == null) && (msgList != null) && (!msgList.isEmpty())) {
                vertex = instantiateVertex();
                V vertexValue = BspUtils.<V>createVertexValue(getConf());
                vertex.setVertexValue(vertexValue);
            }
        }

        if (vertexChanges != null &&
                !vertexChanges.getAddedEdgeList().isEmpty()) {
            MutableVertex<I, V, E, M> mutableVertex =
                (MutableVertex<I, V, E, M>) vertex;
            for (Edge<I, E> edge : vertexChanges.getAddedEdgeList()) {
                edge.setConf(getConf());
                mutableVertex.addEdge(edge);
            }
        }

        return vertex;
    }

    @Override
    public MutableVertex<I, V, E, M> instantiateVertex() {
        return BspUtils.<I, V, E, M>createVertex(getConf());
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
