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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Default implementation of how to resolve vertex creation/removal, messages
 * to nonexistent vertices, etc.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class DefaultVertexResolver<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements VertexResolver<I, V, E> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(
      DefaultVertexResolver.class);

  /** Whether to create vertices when they receive a message */
  private boolean createVertexesOnMessages = true;

  @Override
  public Vertex<I, V, E> resolve(
      I vertexId,
      Vertex<I, V, E> vertex,
      VertexChanges<I, V, E> vertexChanges,
      boolean hasMessages) {
    // This is the default vertex resolution algorithm

    // 1. If the vertex exists, first prune the edges
    removeEdges(vertex, vertexChanges);

    // 2. If vertex removal desired, remove the vertex.
    vertex = removeVertexIfDesired(vertex, vertexChanges);

    // 3. If creation of vertex desired, pick first vertex
    // 4. If vertex doesn't exist, but got messages or added edges, create
    vertex = addVertexIfDesired(vertexId, vertex, vertexChanges, hasMessages);

    // 5. If edge addition, add the edges
    addEdges(vertex, vertexChanges);

    return vertex;
  }

  /**
   * Remove edges as specifed in changes given.
   *
   * @param vertex Vertex to remove edges from
   * @param vertexChanges contains list of edges to remove.
   */
  protected void removeEdges(Vertex<I, V, E> vertex,
                             VertexChanges<I, V, E> vertexChanges) {
    if (vertex == null) {
      return;
    }
    if (hasEdgeRemovals(vertexChanges)) {
      for (I removedDestVertex : vertexChanges.getRemovedEdgeList()) {
        vertex.removeEdges(removedDestVertex);
      }
    }
  }

  /**
   * Remove the vertex itself if the changes desire it. The actual removal is
   * notified by returning null. That is, this method does not do the actual
   * removal but rather returns null if it should be done.
   *
   * @param vertex Vertex to remove.
   * @param vertexChanges specifies if we should remove vertex
   * @return null if vertex should be removed, otherwise the vertex itself.
   */
  protected Vertex<I, V, E> removeVertexIfDesired(
      Vertex<I, V, E> vertex,
      VertexChanges<I, V, E> vertexChanges) {
    if (hasVertexRemovals(vertexChanges)) {
      vertex = null;
    }
    return vertex;
  }

  /**
   * Add the Vertex if desired. Returns the vertex itself, or null if no vertex
   * added.
   *
   * @param vertexId ID of vertex
   * @param vertex Vertex, if not null just returns it as vertex already exists
   * @param vertexChanges specifies if we should add the vertex
   * @param hasMessages true if this vertex received any messages
   * @return Vertex created or passed in, or null if no vertex should be added
   */
  protected Vertex<I, V, E> addVertexIfDesired(
      I vertexId,
      Vertex<I, V, E> vertex,
      VertexChanges<I, V, E> vertexChanges,
      boolean hasMessages) {
    if (vertex == null) {
      if (hasVertexAdditions(vertexChanges)) {
        vertex = vertexChanges.getAddedVertexList().get(0);
      } else if ((hasMessages && createVertexesOnMessages) ||
                 hasEdgeAdditions(vertexChanges)) {
        vertex = getConf().createVertex();
        vertex.initialize(vertexId, getConf().createVertexValue());
      }
    } else if (hasVertexAdditions(vertexChanges)) {
      LOG.warn("resolve: Tried to add a vertex with id = " +
          vertex.getId() + " when one already " +
          "exists.  Ignoring the add vertex request.");
    }
    return vertex;
  }

  /**
   * Add edges to the Vertex.
   *
   * @param vertex Vertex to add edges to
   * @param vertexChanges contains edges to add
   */
  protected void addEdges(Vertex<I, V, E> vertex,
                          VertexChanges<I, V, E> vertexChanges) {
    if (vertex == null) {
      return;
    }
    if (hasEdgeAdditions(vertexChanges)) {
      for (Edge<I, E> edge : vertexChanges.getAddedEdgeList()) {
        vertex.addEdge(edge);
      }
    }
  }

  /**
   * Check if changes contain vertex removal requests
   *
   * @param changes VertexChanges to check
   * @return true if changes contains vertex removal requests
   */
  protected  boolean hasVertexRemovals(VertexChanges<I, V, E> changes) {
    return changes != null && changes.getRemovedVertexCount() > 0;
  }

  /**
   * Check if changes contain vertex addition requests
   *
   * @param changes VertexChanges to check
   * @return true if changes contains vertex addition requests
   */
  protected boolean hasVertexAdditions(VertexChanges<I, V, E> changes) {
    return changes != null && !changes.getAddedVertexList().isEmpty();
  }

  /**
   * Check if changes contain edge addition requests
   *
   * @param changes VertexChanges to check
   * @return true if changes contains edge addition requests
   */
  protected boolean hasEdgeAdditions(VertexChanges<I, V, E> changes) {
    return changes != null && !changes.getAddedEdgeList().isEmpty();
  }

  /**
   * Check if changes contain edge removal requests
   *
   * @param changes VertexChanges to check
   * @return true if changes contains edge removal requests
   */
  protected boolean hasEdgeRemovals(VertexChanges<I, V, E> changes) {
    return changes != null && !changes.getRemovedEdgeList().isEmpty();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    createVertexesOnMessages = conf.getResolverCreateVertexOnMessages();
  }
}
