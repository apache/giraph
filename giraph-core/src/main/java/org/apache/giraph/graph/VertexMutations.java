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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Structure to hold all the possible graph mutations that can occur during a
 * superstep.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class VertexMutations<I extends WritableComparable,
    V extends Writable, E extends Writable> implements VertexChanges<I, V, E>,
    Writable, ImmutableClassesGiraphConfigurable {
  /** List of added vertices during the last superstep */
  private final List<Vertex<I, V, E>> addedVertexList =
      Lists.newArrayList();
  /** Count of remove vertex requests */
  private int removedVertexCount = 0;
  /** List of added edges */
  private final List<Edge<I, E>> addedEdgeList = Lists.newArrayList();
  /** List of removed edges */
  private final List<I> removedEdgeList = Lists.newArrayList();
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E> conf;

  /**
   * Copy the vertex mutations.
   *
   * @return Copied vertex mutations
   */
  public VertexMutations<I, V, E> copy() {
    VertexMutations<I, V, E> copied = new VertexMutations<I, V, E>();
    copied.addedVertexList.addAll(this.addedVertexList);
    copied.removedVertexCount = this.removedVertexCount;
    copied.addedEdgeList.addAll(this.addedEdgeList);
    copied.removedEdgeList.addAll(this.removedEdgeList);
    copied.conf = this.conf;
    return copied;
  }

  @Override
  public List<Vertex<I, V, E>> getAddedVertexList() {
    return addedVertexList;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    addedVertexList.clear();
    addedEdgeList.clear();
    removedEdgeList.clear();

    int addedVertexListSize = input.readInt();
    for (int i = 0; i < addedVertexListSize; ++i) {
      Vertex<I, V, E> vertex =
          WritableUtils.readVertexFromDataInput(input, getConf());
      addedVertexList.add(vertex);
    }
    removedVertexCount = input.readInt();
    int addedEdgeListSize = input.readInt();
    for (int i = 0; i < addedEdgeListSize; ++i) {
      Edge<I, E> edge = conf.createEdge();
      WritableUtils.readEdge(input, edge);
      addedEdgeList.add(edge);
    }
    int removedEdgeListSize = input.readInt();
    for (int i = 0; i < removedEdgeListSize; ++i) {
      I removedEdge = conf.createVertexId();
      removedEdge.readFields(input);
      removedEdgeList.add(removedEdge);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(addedVertexList.size());
    for (Vertex<I, V, E> vertex : addedVertexList) {
      WritableUtils.writeVertexToDataOutput(output, vertex, getConf());
    }
    output.writeInt(removedVertexCount);
    output.writeInt(addedEdgeList.size());
    for (Edge<I, E> edge : addedEdgeList) {
      edge.getTargetVertexId().write(output);
      edge.getValue().write(output);
    }
    output.writeInt(removedEdgeList.size());
    for (I removedEdge : removedEdgeList) {
      removedEdge.write(output);
    }
  }

  /**
   * Add a vertex mutation
   *
   * @param vertex Vertex to be added
   */
  public void addVertex(Vertex<I, V, E> vertex) {
    addedVertexList.add(vertex);
  }

  @Override
  public int getRemovedVertexCount() {
    return removedVertexCount;
  }

  /**
   * Removed a vertex mutation (increments a count)
   */
  public void removeVertex() {
    ++removedVertexCount;
  }

  @Override
  public List<Edge<I, E>> getAddedEdgeList() {
    return addedEdgeList;
  }

  /**
   * Add an edge to this vertex
   *
   * @param edge Edge to be added
   */
  public void addEdge(Edge<I, E> edge) {
    addedEdgeList.add(edge);
  }

  @Override
  public List<I> getRemovedEdgeList() {
    return removedEdgeList;
  }

  /**
   * Remove an edge on this vertex
   *
   * @param destinationVertexId Vertex index of the destination of the edge
   */
  public void removeEdge(I destinationVertexId) {
    removedEdgeList.add(destinationVertexId);
  }

  /**
   * Add one vertex mutations to another
   *
   * @param vertexMutations Object to be added
   */
  public void addVertexMutations(VertexMutations<I, V, E> vertexMutations) {
    addedVertexList.addAll(vertexMutations.getAddedVertexList());
    removedVertexCount += vertexMutations.getRemovedVertexCount();
    addedEdgeList.addAll(vertexMutations.getAddedEdgeList());
    removedEdgeList.addAll(vertexMutations.getRemovedEdgeList());
  }

  @Override
  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("added vertices", getAddedVertexList().toString());
      jsonObject.put("added edges", getAddedEdgeList().toString());
      jsonObject.put("removed vertex count", getRemovedVertexCount());
      jsonObject.put("removed edges", getRemovedEdgeList().toString());
      return jsonObject.toString();
    } catch (JSONException e) {
      throw new IllegalStateException("toString: Got a JSON exception",
          e);
    }
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }
}
