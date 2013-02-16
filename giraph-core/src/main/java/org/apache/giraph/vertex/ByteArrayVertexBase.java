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

import org.apache.giraph.graph.Edge;
import org.apache.giraph.utils.ByteArrayEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Common base class for byte-array backed vertices.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class ByteArrayVertexBase<
    I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> {
  /** Serialized edge list. */
  private ByteArrayEdges<I, E> edges;

  /**
   * Append an edge to the serialized representation.
   *
   * @param edge Edge to append
   */
  protected void appendEdge(Edge<I, E> edge) {
    edges.appendEdge(edge);
  }

  /**
   * Remove the first edge pointing to a target vertex.
   *
   * @param targetVertexId Target vertex id
   * @return True if one such edge was found and removed.
   */
  protected boolean removeFirstEdge(I targetVertexId) {
    return edges.removeFirstEdge(targetVertexId);
  }

  /**
   * Remove all edges pointing to a target vertex.
   *
   * @param targetVertexId Target vertex id
   * @return The number of removed edges
   */
  protected int removeAllEdges(I targetVertexId) {
    return edges.removeAllEdges(targetVertexId);
  }

  @Override
  public final void setEdges(Iterable<Edge<I, E>> edges) {
    // If the edge iterable is backed by a byte-array,
    // we simply get a shallow copy of it.
    if (edges instanceof ByteArrayEdges) {
      this.edges = new ByteArrayEdges<I, E>((ByteArrayEdges<I, E>) edges);
    } else {
      this.edges = new ByteArrayEdges<I, E>(getConf());
      this.edges.setEdges(edges);
    }
  }

  @Override
  public final Iterable<Edge<I, E>> getEdges() {
    return edges;
  }

  @Override
  public final int getNumEdges() {
    return edges.getNumEdges();
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    I vertexId = getId();
    if (vertexId == null) {
      vertexId = getConf().createVertexId();
    }
    vertexId.readFields(in);

    V vertexValue = getValue();
    if (vertexValue == null) {
      vertexValue = getConf().createVertexValue();
    }
    vertexValue.readFields(in);

    initialize(vertexId, vertexValue);

    edges.readFields(in);

    readHaltBoolean(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    edges.write(out);

    out.writeBoolean(isHalted());
  }
}
