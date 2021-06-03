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

package org.apache.giraph.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.CreateSourceVertexCallback;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicCollectionsUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

/**
 * TestGraph class for in-memory testing.
 *
 * @param <I> Vertex index type
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class TestGraph<I extends WritableComparable,
                       V extends Writable,
                       E extends Writable>
                       implements Iterable<Vertex<I, V, E>> {
  /** Vertex value combiner */
  protected final VertexValueCombiner<V> vertexValueCombiner;
  /** The vertex values */
  protected Basic2ObjectMap<I, Vertex<I, V, E>> vertices;
  /** The configuration */
  protected ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Callback that makes a decision on whether vertex should be created */
  private CreateSourceVertexCallback<I> createSourceVertexCallback;

  /**
   * Constructor requiring classes
   *
   * @param conf Should have vertex and edge classes set.
   */
  public TestGraph(GiraphConfiguration conf) {
    this.conf = new ImmutableClassesGiraphConfiguration<>(conf);
    createSourceVertexCallback =
        GiraphConstants.CREATE_EDGE_SOURCE_VERTICES_CALLBACK
            .newInstance(this.conf);
    vertexValueCombiner = this.conf.createVertexValueCombiner();
    vertices = BasicCollectionsUtils.create2ObjectMap(
      this.conf.getVertexIdClass()
    );
  }

  public Collection<Vertex<I, V, E>> getVertices() {
    return vertices.values();
  }

  public int getVertexCount() {
    return vertices.size();
  }

  public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
    return conf;
  }

  /**
   * Clear all data
   *
   */
  public void clear() {
    vertices.clear();
  }

  /**
   * Add vertex
   *
   * @param vertex Vertex
   * @return this
   */
  public TestGraph<I, V, E> addVertex(Vertex<I, V, E> vertex) {
    Vertex<I, V, E> previousVertex = vertices.get(vertex.getId());
    if (previousVertex != null) {
      vertexValueCombiner.combine(previousVertex.getValue(), vertex.getValue());
      for (Edge<I, E> edge : vertex.getEdges()) {
        previousVertex.addEdge(edge);
      }
    } else {
      vertices.put(vertex.getId(), vertex);
    }
    return this;
  }

  /**
   * Add vertex with given ID
   *
   * @param id the index
   * @param value the value
   * @param edges all edges
   * @return this
   */
  public TestGraph<I, V, E> addVertex(I id, V value,
                                      Entry<I, E>... edges) {
    addVertex(makeVertex(id, value, edges));
    return this;
  }

  /**
   * Set vertex, replace if there was already a vertex with same id added
   *
   * @param vertex Vertex
   * @return this
   */
  public TestGraph<I, V, E> setVertex(Vertex<I, V, E> vertex) {
    vertices.put(vertex.getId(), vertex);
    return this;
  }

  /**
   * Set vertex, replace if there was already a vertex with same id added
   *
   * @param id the index
   * @param value the value
   * @param edges all edges
   * @return this
   */
  public TestGraph<I, V, E> setVertex(I id, V value, Entry<I, E>... edges) {
    setVertex(makeVertex(id, value, edges));
    return this;
  }

  /**
   * Add an edge to an existing vertex
   *`
   * @param vertexId Edge origin
   * @param edgePair The edge
   * @return this
   */
  public TestGraph<I, V, E> addEdge(I vertexId, Entry<I, E> edgePair) {
    return addEdge(vertexId, edgePair.getKey(), edgePair.getValue());
  }

  /**
   * Add an edge to an existing vertex
   *
   * @param vertexId Edge origin
   * @param toVertex Edge destination
   * @param edgeValue Edge value
   * @return this
   */
  public TestGraph<I, V, E> addEdge(I vertexId, I toVertex, E edgeValue) {
    if (!vertices.containsKey(vertexId)) {
      if (createSourceVertexCallback.shouldCreateSourceVertex(vertexId)) {
        Vertex<I, V, E> v = conf.createVertex();
        v.initialize(vertexId, conf.createVertexValue());
        vertices.put(vertexId, v);
      }
    }
    Vertex<I, V, E> v = vertices.get(vertexId);
    if (v != null) {
      v.addEdge(EdgeFactory.create(toVertex, edgeValue));
    }
    return this;
  }

  /**
   * An iterator over the vertices
   *
   * @return the iterator
   */
  @Override
  public Iterator<Vertex<I, V, E>> iterator() {
    return vertices.valueIterator();
  }

  /**
   * Return a given vertex
   *
   * @param id the id
   * @return the value
   */
  public Vertex<I, V, E> getVertex(I id) {
    return vertices.get(id);
  }

  /**
   * Create edges for given ids
   *
   * @param destEdgess ids to which the edges link
   * @return an iterable containing the edges
   */
  protected Iterable<Edge<I, E>>
  createEdges(Entry<I, E>... destEdgess) {
    List<Edge<I, E>> edgesList = Lists.newArrayList();
    for (Entry<I, E> e: destEdgess) {
      edgesList.add(EdgeFactory.create(e.getKey(), e.getValue()));
    }
    return edgesList;
  }

  /**
   * Create a vertex
   *
   * @param id the id of the vertex
   * @param value the vertex value
   * @param edges edges to other vertices
   * @return a new vertex
   */
  protected Vertex<I, V, E> makeVertex(I id, V value,
      Entry<I, E>... edges) {
    Vertex<I, V, E> vertex = conf.createVertex();
    vertex.initialize(id, value, createEdges(edges));
    return vertex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add(
      "vertices", vertices).toString();
  }
}
