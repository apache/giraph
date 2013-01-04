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

package org.apache.giraph.vertex;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
    implements WorkerAggregatorUsage, Writable,
    ImmutableClassesGiraphConfigurable<I, V, E, M> {
  /** Vertex id. */
  private I id;
  /** Vertex value. */
  private V value;
  /** If true, do not do anymore computation on this vertex. */
  private boolean halt;
  /** Global graph state **/
  private GraphState<I, V, E, M> graphState;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;

  /**
   * This method must be called after instantiation of a vertex
   * with ImmutableClassesGiraphConfiguration
   * unless deserialization from readFields() is
   * called.
   *
   * @param id Will be the vertex id
   * @param value Will be the vertex value
   * @param edges Iterable of edges
   */
  public void initialize(I id, V value, Iterable<Edge<I, E>> edges) {
    this.id = id;
    this.value = value;
    setEdges(edges);
  }

  /**
   * This method only sets id and value. Can be used by Vertex
   * implementations in readFields().
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  public void initialize(I id, V value) {
    this.id = id;
    this.value = value;
    setEdges(Collections.<Edge<I, E>>emptyList());
  }

  /**
   * Set the outgoing edges for this vertex.
   *
   * @param edges Iterable of edges
   */
  public abstract void setEdges(Iterable<Edge<I, E>> edges);

  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   *                 a life expectancy as long as next() is not called.
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
    if (graphState.getWorkerClientRequestProcessor().
          sendMessageRequest(id, message)) {
      graphState.getGraphMapper().notifySentMessages();
    }
  }

  /**
   * Lookup WorkerInfo for myself.
   *
   * @return WorkerInfo about worker holding this Vertex.
   */
  public WorkerInfo getMyWorkerInfo() {
    return getVertexWorkerInfo(id);
  }

  /**
   * Lookup WorkerInfo for a Vertex.
   *
   * @param vertexId VertexId to lookup
   * @return WorkerInfo about worker holding this Vertex.
   */
  public WorkerInfo getVertexWorkerInfo(I vertexId) {
    return getVertexPartitionOwner(vertexId).getWorkerInfo();
  }

  /**
   * Lookup PartitionOwner for a Vertex
   *
   * @param vertexId id of Vertex to look up.
   * @return PartitionOwner holding Vertex
   */
  private PartitionOwner getVertexPartitionOwner(I vertexId) {
    return getGraphState().getWorkerClientRequestProcessor().
        getVertexPartitionOwner(vertexId);
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
   * Get the graph state for all workers.
   *
   * @return Graph state for all workers
   */
  public GraphState<I, V, E, M> getGraphState() {
    return graphState;
  }

  /**
   * Set the graph state for all workers
   *
   * @param graphState Graph state for all workers
   */
  public void setGraphState(GraphState<I, V, E, M> graphState) {
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
    getGraphState().getWorkerAggregatorUsage().
        aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return getGraphState().getWorkerAggregatorUsage().
        <A>getAggregatedValue(name);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    I vertexId = (I) getConf().createVertexId();
    vertexId.readFields(in);
    V vertexValue = (V) getConf().createVertexValue();
    vertexValue.readFields(in);

    int numEdges = in.readInt();
    List<Edge<I, E>> edges = Lists.newArrayListWithCapacity(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = (I) getConf().createVertexId();
      targetVertexId.readFields(in);
      E edgeValue = (E) getConf().createEdgeValue();
      edgeValue.readFields(in);
      edges.add(new Edge<I, E>(targetVertexId, edgeValue));
    }

    initialize(vertexId, vertexValue, edges);

    readHaltBoolean(in);
  }

  /**
   * Helper method for subclasses which implement their own readFields() to use.
   *
   * @param in DataInput to read from.
   * @throws IOException If anything goes wrong during read.
   */
  protected void readHaltBoolean(DataInput in) throws IOException {
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

    out.writeBoolean(halt);
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, V, E, M> getConf() {
    return conf;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<I, V, E, M> conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getId() + ",value=" + getValue() +
        ",#edges=" + getNumEdges() + ")";
  }
}
