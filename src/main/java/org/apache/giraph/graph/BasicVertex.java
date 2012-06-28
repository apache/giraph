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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Basic interface for writing a BSP application for computation.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public abstract class BasicVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements AggregatorUsage, Iterable<I>, Writable, Configurable {
  /** If true, do not do anymore computation on this vertex. */
  protected boolean halt = false;
  /** Global graph state **/
  private GraphState<I, V, E, M> graphState;
  /** Configuration */
  private Configuration conf;


  /**
   * This method must be called after instantiation of a vertex with BspUtils
   * unless deserialization from readFields() is called.
   *
   * @param vertexId Will be the vertex id
   * @param vertexValue Will be the vertex value
   * @param edges A map of destination edge ids to edge values (can be null)
   * @param messages Initial messages for this vertex (can be null)
   */
  public abstract void initialize(
      I vertexId, V vertexValue, Map<I, E> edges, Iterable<M> messages);

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
   * Get the vertex id.
   *
   * @return My vertex id.
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
   * Get a read-only view of the out-edges of this vertex.
   *
   * @return the out edges (sort order determined by subclass implementation).
   */
  @Override
  public abstract Iterator<I> iterator();

  /**
   * Get the edge value associated with a target vertex id.
   *
   * @param targetVertexId Target vertex id to check
   *
   * @return the value of the edge to targetVertexId (or null if there
   *         is no edge to it)
   */
  public abstract E getEdgeValue(I targetVertexId);

  /**
   * Does an edge with the target vertex id exist?
   *
   * @param targetVertexId Target vertex id to check
   * @return true if there is an edge to the target
   */
  public abstract boolean hasEdge(I targetVertexId);

  /**
   * Get the number of outgoing edges on this vertex.
   *
   * @return the total number of outbound edges from this vertex
   */
  public abstract int getNumOutEdges();

  /**
   * Send a message to a vertex id.  The message should not be mutated after
   * this method returns or else undefined results could occur.
   *
   * @param id Vertex id to send the message to
   * @param msg Message data to send.  Note that after the message is sent,
   *        the user should not modify the object.
   */
  public void sendMsg(I id, M msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsg: Cannot send null message to " + id);
    }
    getGraphState().getWorkerCommunications().
    sendMessageReq(id, msg);
  }

  /**
   * Send a message to all edges.
   *
   * @param msg Message sent to all edges.
   */
  public void sendMsgToAllEdges(M msg) {
    for (Iterator<I> edges = iterator(); edges.hasNext();) {
      sendMsg(edges.next(), msg);
    }
  }

  /**
   * After this is called, the compute() code will no longer be called for
   * this vertex unless a message is sent to it.  Then the compute() code
   * will be called once again until this function is called.  The
   * application finishes only when all vertices vote to halt.
   */
  public void voteToHalt() {
    halt = true;
  }

  /**
   * Is this vertex done?
   *
   * @return True if halted, false otherwise.
   */
  public boolean isHalted() {
    return halt;
  }

  /**
   *  Get the list of incoming messages from the previous superstep.  Same as
   *  the message iterator passed to compute().
   *
   *  @return Iterator of messages.
   */
  public abstract Iterable<M> getMessages();

  /**
   * Copy the messages this vertex should process in the current superstep
   *
   * @param messages the messages sent to this vertex in the previous superstep
   */
  abstract void putMessages(Iterable<M> messages);

  /**
   * Release unnecessary resources (will be called after vertex returns from
   * {@link #compute()})
   */
  abstract void releaseResources();

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

  /**
   * Get the worker context
   *
   * @return WorkerContext context
   */
  public WorkerContext getWorkerContext() {
    return getGraphState().getGraphMapper().getWorkerContext();
  }

  @Override
  public final <A extends Writable> Aggregator<A> registerAggregator(
    String name, Class<? extends Aggregator<A>> aggregatorClass)
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

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
