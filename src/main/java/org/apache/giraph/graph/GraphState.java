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
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Global state of the graph.  Should be treated as a singleton (but is kept
 * as a regular bean to facilitate ease of unit testing)
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
@SuppressWarnings("rawtypes")
public class GraphState<I extends WritableComparable, V extends Writable,
        E extends Writable, M extends Writable> {
    /** Graph-wide superstep */
    private long superstep = 0;
    /** Graph-wide number of vertices */
    private long numVertices = -1;
    /** Graph-wide number of edges */
    private long numEdges = -1;
    /** Graph-wide map context */
    private Mapper.Context context = null;
    /** Graph-wide BSP Mapper for this Vertex */
    private GraphMapper<I, V, E, M> graphMapper = null;

    public long getSuperstep() {
        return superstep;
    }

    public GraphState<I, V, E, M> setSuperstep(long superstep) {
        this.superstep = superstep;
        return this;
    }

    public long getNumVertices() {
        return numVertices;
    }

    public GraphState<I, V, E, M> setNumVertices(long numVertices) {
        this.numVertices = numVertices;
        return this;
    }

    public long getNumEdges() {
        return numEdges;
    }

    public GraphState<I, V, E, M> setNumEdges(long numEdges) {
        this.numEdges = numEdges;
        return this;
    }

    public Mapper.Context getContext() {
        return context;
    }

    public GraphState<I, V , E ,M> setContext(Mapper.Context context) {
        this.context = context;
        return this;
    }

    public GraphMapper<I, V, E, M> getGraphMapper() {
        return graphMapper;
    }

    public GraphState<I, V, E, M> setGraphMapper(
            GraphMapper<I, V, E, M> graphMapper) {
        this.graphMapper = graphMapper;
        return this;
    }
}
