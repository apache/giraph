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
package org.apache.giraph.jython;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Base class for writing computations in Jython.
 *
 * Note that this class DOES NOT implement
 * {@link org.apache.giraph.graph.Computation}.
 * This is because we want to support passing in pure Jython types,
 * and implementing the {@link org.apache.giraph.graph.Computation}
 * requires passing in {@link Writable}s.
 * Calling such methods from Jython would throw errors. So, instead,
 * we have recreated the methods with the same name here. In each method
 * we check if the type is a pure Jython value, and if so wrap it in
 * the necessary
 * {@link org.apache.giraph.jython.wrappers.JythonWritableWrapper}.
 *
 * This class works together with {@link JythonGiraphComputation} which takes
 * care of the {@link org.apache.giraph.graph.Computation}
 * Giraph infrastructure side of things.
 */
public abstract class JythonComputation extends
    DefaultImmutableClassesGiraphConfigurable {
  /** The computation to callback to */
  private JythonGiraphComputation giraphCompute;

  public void setGiraphCompute(JythonGiraphComputation giraphCompute) {
    this.giraphCompute = giraphCompute;
  }

  /**
   * User's computation function
   *
   * @param vertex the Vertex to compute on
   * @param messages iterable of messages
   */
  public abstract void compute(Object vertex, Iterable messages);

  /**
   * Prepare for computation. This method is executed exactly once prior to
   * {@link #compute(Object, Iterable)} being called for any of the vertices
   * in the partition.
   */
  public void preSuperstep() { }

  /**
   * Finish computation. This method is executed exactly once after computation
   * for all vertices in the partition is complete.
   */
  public void postSuperstep() { }

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  public long getSuperstep() {
    return giraphCompute.getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public long getTotalNumVertices() {
    return giraphCompute.getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public long getTotalNumEdges() {
    return giraphCompute.getTotalNumEdges();
  }

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  public void sendMessage(Object id, Object message) {
    WritableComparable wrappedId = giraphCompute.wrapIdIfNecessary(id);
    Writable wrappedMessage = giraphCompute.wrapIfNecessary(message,
        GraphType.OUTGOING_MESSAGE_VALUE);
    giraphCompute.sendMessage(wrappedId, wrappedMessage);
  }

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  public void sendMessageToAllEdges(Vertex vertex, Object message) {
    Writable wrappedMessage = giraphCompute.wrapIfNecessary(message,
        GraphType.OUTGOING_MESSAGE_VALUE);
    giraphCompute.sendMessageToAllEdges(vertex, wrappedMessage);
  }

  /**
   * Send a message to multiple target vertex ids in the iterator.
   *
   * @param vertexIdIterator An iterator to multiple target vertex ids.
   * @param message Message sent to all targets in the iterator.
   */
  public void sendMessageToMultipleEdges(Iterator vertexIdIterator,
      Object message) {
    Writable wrappedMessage = giraphCompute.wrapIfNecessary(message,
        GraphType.OUTGOING_MESSAGE_VALUE);
    giraphCompute.sendMessageToMultipleEdges(vertexIdIterator, wrappedMessage);
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param vertexValue Vertex value
   * @param edges Initial edges
   */
  public void addVertexRequest(Object id, Object vertexValue,
      OutEdges edges) throws IOException {
    WritableComparable wrappedId = giraphCompute.wrapIdIfNecessary(id);
    Writable wrappedValue = giraphCompute.wrapIfNecessary(vertexValue,
        GraphType.VERTEX_VALUE);
    giraphCompute.addVertexRequest(wrappedId, wrappedValue, edges);
  }

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param vertexValue Vertex value
   */
  public void addVertexRequest(Object id, Object vertexValue)
    throws IOException {
    WritableComparable wrappedId = giraphCompute.wrapIdIfNecessary(id);
    Writable wrappedVertexValue = giraphCompute.wrapIfNecessary(vertexValue,
        GraphType.VERTEX_VALUE);
    giraphCompute.addVertexRequest(wrappedId, wrappedVertexValue);
  }

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param id Id of the vertex to be removed.
   */
  public void removeVertexRequest(Object id) throws IOException {
    WritableComparable wrappedId = giraphCompute.wrapIdIfNecessary(id);
    giraphCompute.removeVertexRequest(wrappedId);
  }

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next superstep)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   */
  public void addEdgeRequest(Object sourceVertexId, Edge edge)
    throws IOException {
    WritableComparable wrappedSourceId =
        giraphCompute.wrapIdIfNecessary(sourceVertexId);
    giraphCompute.addEdgeRequest(wrappedSourceId, edge);
  }

  /**
   * Request to remove all edges from a given source vertex to a given target
   * vertex (processed just prior to the next superstep).
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   */
  public void removeEdgesRequest(Object sourceVertexId, Object targetVertexId)
    throws IOException {
    WritableComparable wrappedSourceVertexId =
        giraphCompute.wrapIdIfNecessary(sourceVertexId);
    WritableComparable wrappedTargetVertexId =
        giraphCompute.wrapIdIfNecessary(targetVertexId);
    giraphCompute.removeEdgesRequest(wrappedSourceVertexId,
        wrappedTargetVertexId);
  }

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  public Mapper.Context getContext() {
    return giraphCompute.getContext();
  }

  /**
   * Get the worker context
   *
   * @param <W> WorkerContext class
   * @return WorkerContext context
   */
  @SuppressWarnings("unchecked")
  public <W extends WorkerContext> W getWorkerContext() {
    return (W) giraphCompute.getWorkerContext();
  }
}

