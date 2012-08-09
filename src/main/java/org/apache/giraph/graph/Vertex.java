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

import com.google.common.collect.Iterables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public abstract class Vertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerAggregatorUsage, Writable, Configurable {
  /** Vertex id. */
  private I id;
  /** Vertex value. */
  private V value;
  /** If true, do not do anymore computation on this vertex. */
  private boolean halt;
  /** Global graph state **/
  private GraphState<I, V, E, M> graphState;
  /** Configuration */
  private Configuration conf;


  /**
   * This method must be called after instantiation of a vertex with BspUtils
   * unless deserialization from readFields() is called.
   *
   * @param id Will be the vertex id
   * @param value Will be the vertex value
   * @param edges A map of destination edge ids to edge values (can be null)
   * @param messages Initial messages for this vertex (can be null)
   */
  public abstract void initialize(
      I id, V value, Map<I, E> edges, Iterable<M> messages);

  /**
   * This method must be called once by the subclass's initialize() or by
   * readFields() in order to set id and value.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  public void initialize(I id, V value) {
    this.id = id;
    this.value = value;
  }

  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep
   * @throws IOException
   */
  public abstract void compute(Iterable<M> messages) throws IOException;

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
  public I getId() {
    return id;
  }

  /**
   * Get the vertex value (data stored with vertex)
   *
   * @return Vertex value
   */
  public V getValue() {
    return value;
  }

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param value Vertex data to be set
   */
  public void setValue(V value) {
    this.value = value;
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public long getTotalNumVertices() {
    return getGraphState().getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public long getTotalNumEdges() {
    return getGraphState().getTotalNumEdges();
  }

  /**
   * Get a read-only view of the out-edges of this vertex.
   *
   * @return the out edges (sort order determined by subclass implementation).
   */
  public abstract Iterable<Edge<I, E>> getEdges();

  /**
   * Does an edge with the target vertex id exist?
   *
   * @param targetVertexId Target vertex id to check
   * @return true if there is an edge to the target
   */
  public boolean hasEdge(I targetVertexId) {
    for (Edge<I, E> edge : getEdges()) {
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the edge value associated with a target vertex id.
   *
   * @param targetVertexId Target vertex id to check
   *
   * @return the value of the edge to targetVertexId (or null if there
   *         is no edge to it)
   */
  public E getEdgeValue(I targetVertexId) {
    for (Edge<I, E> edge : getEdges()) {
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        return edge.getValue();
      }
    }
    return null;
  }

  /**
   * Get the number of outgoing edges on this vertex.
   *
   * @return the total number of outbound edges from this vertex
   */
  public int getNumEdges() {
    return Iterables.size(getEdges());
  }

  /**
   * Send a message to a vertex id.  The message should not be mutated after
   * this method returns or else undefined results could occur.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send.  Note that after the message is sent,
   *        the user should not modify the object.
   */
  public void sendMessage(I id, M message) {
    if (message == null) {
      throw new IllegalArgumentException(
          "sendMessage: Cannot send null message to " + id);
    }
    getGraphState().getWorkerCommunications().
        sendMessageRequest(id, message);
  }

  /**
   * Send a message to all edges.
   *
   * @param message Message sent to all edges.
   */
  public void sendMessageToAllEdges(M message) {
    for (Edge<I, E> edge : getEdges()) {
      sendMessage(edge.getTargetVertexId(), message);
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
   * Re-activate vertex if halted.
   */
  public void wakeUp() {
    halt = false;
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
   *  @return Messages received.
   */
  public abstract Iterable<M> getMessages();

  /**
   * Get the number of messages from the previous superstep.
   * @return Number of messages received.
   */
  public int getNumMessages() {
    return Iterables.size(getMessages());
  }

  /**
   * Copy the messages this vertex should process in the current superstep
   *
   * @param messages the messages sent to this vertex in the previous superstep
   */
  abstract void putMessages(Iterable<M> messages);

  /**
   * Release unnecessary resources (will be called after vertex returns from
   * {@link #compute(Iterable)})
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
  public <A extends Writable> void aggregate(String name, A value) {
    getGraphState().getGraphMapper().getWorkerAggregatorUsage().
        aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return getGraphState().getGraphMapper().getWorkerAggregatorUsage().
        <A>getAggregatedValue(name);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    I vertexId = BspUtils.<I>createVertexId(getConf());
    vertexId.readFields(in);
    V vertexValue = BspUtils.<V>createVertexValue(getConf());
    vertexValue.readFields(in);

    int numEdges = in.readInt();
    Map<I, E> edges = new HashMap<I, E>(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = BspUtils.<I>createVertexId(getConf());
      targetVertexId.readFields(in);
      E edgeValue = BspUtils.<E>createEdgeValue(getConf());
      edgeValue.readFields(in);
      edges.put(targetVertexId, edgeValue);
    }

    int numMessages = in.readInt();
    List<M> messages = new ArrayList<M>(numMessages);
    for (int i = 0; i < numMessages; ++i) {
      M message = BspUtils.<M>createMessageValue(getConf());
      message.readFields(in);
      messages.add(message);
    }
    initialize(vertexId, vertexValue, edges, messages);

    halt = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(getNumEdges());
    for (Edge<I, E> edge : getEdges()) {
      edge.getTargetVertexId().write(out);
      edge.getValue().write(out);
    }

    out.writeInt(getNumMessages());
    for (M message : getMessages()) {
      message.write(out);
    }

    out.writeBoolean(halt);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getId() + ",value=" + getValue() +
        ",#edges=" + getNumEdges() + ")";
  }
}
