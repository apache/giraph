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

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Vertex which only contains ID.
 *
 * @param <I> Vertex id type
 */
public class OnlyIdVertex<I extends WritableComparable>
    extends DefaultImmutableClassesGiraphConfigurable<I, Writable, Writable>
    implements Vertex<I, Writable, Writable> {
  /** Vertex id. */
  private I id;

  @Override
  public void initialize(
      I id, Writable value, Iterable<Edge<I, Writable>> edges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initialize(I id, Writable value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public I getId() {
    return id;
  }

  public void setId(I id) {
    this.id = id;
  }

  @Override
  public Writable getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValue(Writable value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void voteToHalt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumEdges() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Edge<I, Writable>> getEdges() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEdges(Iterable<Edge<I, Writable>> edges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<MutableEdge<I, Writable>> getMutableEdges() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writable getEdgeValue(I targetVertexId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEdgeValue(I targetVertexId, Writable edgeValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Writable> getAllEdgeValues(I targetVertexId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addEdge(Edge<I, Writable> edge) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeEdges(I targetVertexId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unwrapMutableEdges() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void wakeUp() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isHalted() {
    throw new UnsupportedOperationException();
  }
}
