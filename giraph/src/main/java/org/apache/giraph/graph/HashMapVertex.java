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
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User applications can subclass {@link HashMapVertex}, which stores
 * the outbound edges in a HashMap, for efficient edge random-access.  Note
 * that {@link EdgeListVertex} is much more memory efficient for static graphs.
 * User applications which need to implement their own
 * in-memory data structures should subclass {@link MutableVertex}.
 *
 * Package access will prevent users from accessing internal methods.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class HashMapVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(HashMapVertex.class);
  /** Map of target vertices and their edge values */
  protected Map<I, E> edgeMap = new HashMap<I, E>();

  @Override
  public void setEdges(Map<I, E> edges) {
    edgeMap.putAll(edges);
  }

  @Override
  public boolean addEdge(I targetVertexId, E value) {
    if (edgeMap.put(targetVertexId, value) != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addEdge: Vertex=" + getId() +
            ": already added an edge value for target vertex id " +
            targetVertexId);
      }
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean hasEdge(I targetVertexId) {
    return edgeMap.containsKey(targetVertexId);
  }

  /**
   * Get an iterator to the edges on this vertex.
   *
   * @return A <em>sorted</em> iterator, as defined by the sort-order
   *         of the vertex ids
   */
  @Override
  public Iterable<Edge<I, E>> getEdges() {
    return Iterables.transform(edgeMap.entrySet(),
        new Function<Map.Entry<I, E>, Edge<I, E>>() {

          @Override
          public Edge<I, E> apply(Map.Entry<I, E> edge) {
            return new Edge<I, E>(edge.getKey(), edge.getValue());
          }
        });
  }

  @Override
  public E getEdgeValue(I targetVertexId) {
    return edgeMap.get(targetVertexId);
  }

  @Override
  public int getNumEdges() {
    return edgeMap.size();
  }

  @Override
  public E removeEdge(I targetVertexId) {
    return edgeMap.remove(targetVertexId);
  }

  @Override
  public final void sendMessageToAllEdges(M message) {
    for (I targetVertexId : edgeMap.keySet()) {
      sendMessage(targetVertexId, message);
    }
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    I vertexId = getConf().createVertexId();
    vertexId.readFields(in);
    V vertexValue = getConf().createVertexValue();
    vertexValue.readFields(in);
    initialize(vertexId, vertexValue);

    int numEdges = in.readInt();
    edgeMap = Maps.newHashMapWithExpectedSize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = getConf().createVertexId();
      targetVertexId.readFields(in);
      E edgeValue = getConf().createEdgeValue();
      edgeValue.readFields(in);
      edgeMap.put(targetVertexId, edgeValue);
    }

    readHaltBoolean(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(edgeMap.size());
    for (Map.Entry<I, E> edge : edgeMap.entrySet()) {
      edge.getKey().write(out);
      edge.getValue().write(out);
    }

    out.writeBoolean(isHalted());
  }

}

