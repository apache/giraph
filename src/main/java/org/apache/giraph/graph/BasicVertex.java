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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

/**
 * Basic interface for writing a BSP application for computation.
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
@SuppressWarnings("rawtypes")
public abstract class BasicVertex<I extends WritableComparable,
        V extends Writable, E extends Writable, M extends Writable>
        implements AggregatorUsage {
    /** Global graph state **/
    private GraphState<I,V,E,M> graphState;

    /**
     * Optionally defined by the user to be executed once on all workers
     * before application has started.
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public abstract void preApplication()
        throws InstantiationException, IllegalAccessException;

    /**
     * Optionally defined by the user to be executed once on all workers
     * after the application has completed.
     */
    public abstract void postApplication();

    /**
     * Optionally defined by the user to be executed once prior to vertex
     * processing on a worker for the current superstep.
     */
    public abstract void preSuperstep();

    /**
     * Optionally defined by the user to be executed once after all vertex
     * processing on a worker for the current superstep.
     */
    public abstract void postSuperstep();

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param msgIterator Iterator to the messages that were sent to this
     *        vertex in the previous superstep
     * @throws IOException
     */
    public abstract void compute(Iterator<M> msgIterator) throws IOException;

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    public long getSuperstep() {
        return getGraphState().getSuperstep();
    }

    /**
     * Get the vertex id
     */
    public abstract I getVertexId();

    /**
     * Get the vertex value (data stored with vertex)
     *
     * @return Vertex value
     */
    public abstract V getVertexValue();

    /**
     * Set the vertex data (immediately visible in the computation)
     *
     * @param vertexValue Vertex data to be set
     */
    public abstract void setVertexValue(V vertexValue);

    /**
     * Get the total (all workers) number of vertices that
     * existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    public long getNumVertices() {
        return getGraphState().getNumVertices();
    }

    /**
     * Get the total (all workers) number of edges that
     * existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    public long getNumEdges() {
        return getGraphState().getNumEdges();
    }

    /**
     * Every vertex has edges to other vertices.  Get a handle to the outward
     * edge set.
     *
     * @return Map of the destination vertex index to the {@link Edge}
     */
    public abstract SortedMap<I, Edge<I, E>> getOutEdgeMap();

    /**
     * Send a message to a vertex id.
     *
     * @param id vertex id to send the message to
     * @param msg message data to send
     */
    public void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException(
                "sendMsg: Cannot send null message to " + id);
        }
        getGraphState().getGraphMapper().getWorkerCommunications().
            sendMessageReq(id, msg);
    }

    /**
     * Send a message to all edges.
     */
    public abstract void sendMsgToAllEdges(M msg);

    /**
     * After this is called, the compute() code will no longer be called for
     * this vertice unless a message is sent to it.  Then the compute() code
     * will be called once again until this function is called.  The application
     * finishes only when all vertices vote to halt.
     */
    public abstract void voteToHalt();

    /**
     * Is this vertex done?
     */
    public abstract boolean isHalted();

    /**
     *  Get the list of incoming messages from the previous superstep.  Same as
     *  the message iterator passed to compute().
     */
    public abstract List<M> getMsgList();

    /**
     * Get the graph state for all workers.
     *
     * @return Graph state for all workers
     */
    GraphState<I, V, E, M> getGraphState() {
        return graphState;
    }

    /**
     * Set the graph state for all workers
     *
     * @param graphState Graph state for all workers
     */
    void setGraphState(GraphState<I, V, E, M> graphState) {
        this.graphState = graphState;
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    public Mapper.Context getContext() {
        return getGraphState().getContext();
    }

    @Override
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            registerAggregator(name, aggregatorClass);
    }

    @Override
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            getAggregator(name);
    }

    @Override
    public final boolean useAggregator(String name) {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            useAggregator(name);
    }
}
