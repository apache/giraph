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

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User applications can subclass {@link EdgeListVertex}, which stores
 * the outbound edges in an ArrayList (less memory as the cost of expensive
 * random-access lookup).  Good for static graphs.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class EdgeListVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(EdgeListVertex.class);
  /** List of edges */
  private List<Edge<I, E>> edgeList = Lists.newArrayList();

  @Override
  public void setEdges(Map<I, E> edges) {
    if (edges != null) {
      for (Map.Entry<I, E> edge : edges.entrySet()) {
        edgeList.add(new Edge<I, E>(edge.getKey(), edge.getValue()));
      }
    }
  }

  @Override
  public Iterable<Edge<I, E>> getEdges() {
    return edgeList;
  }

  @Override
  public boolean addEdge(I targetVertexId, E value) {
    for (Edge<I, E> edge : getEdges()) {
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        LOG.warn("addEdge: Vertex=" + getId() +
            ": already added an edge value for target vertex id " +
            targetVertexId);
        return false;
      }
    }
    edgeList.add(new Edge<I, E>(targetVertexId, value));
    return true;
  }

  @Override
  public int getNumEdges() {
    return edgeList.size();
  }

  @Override
  public E removeEdge(I targetVertexId) {
    for (Iterator<Edge<I, E>> edges = edgeList.iterator(); edges.hasNext();) {
      Edge<I, E> edge = edges.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        E edgeValue = edge.getValue();
        edges.remove();
        return edgeValue;
      }
    }
    return null;
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    I vertexId = getConf().createVertexId();
    vertexId.readFields(in);
    V vertexValue = getConf().createVertexValue();
    vertexValue.readFields(in);
    initialize(vertexId, vertexValue);

    int numEdges = in.readInt();
    edgeList = Lists.newArrayListWithCapacity(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = getConf().createVertexId();
      targetVertexId.readFields(in);
      E edgeValue = getConf().createEdgeValue();
      edgeValue.readFields(in);
      edgeList.add(new Edge<I, E>(targetVertexId, edgeValue));
    }

    readHaltBoolean(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(edgeList.size());
    for (Edge<I, E> edge : edgeList) {
      edge.getTargetVertexId().write(out);
      edge.getValue().write(out);
    }

    out.writeBoolean(isHalted());
  }

}

