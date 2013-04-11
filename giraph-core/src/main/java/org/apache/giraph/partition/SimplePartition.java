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

package org.apache.giraph.partition;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.giraph.conf.GiraphConstants.USE_OUT_OF_CORE_MESSAGES;

/**
 * A simple map-based container that stores vertices.  Vertex ids will map to
 * exactly one partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SimplePartition<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BasicPartition<I, V, E, M> {
  /** Vertex map for this range (keyed by index) */
  private ConcurrentMap<I, Vertex<I, V, E, M>> vertexMap;

  /**
   * Constructor for reflection.
   */
  public SimplePartition() { }

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    super.initialize(partitionId, progressable);
    if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
  }

  @Override
  public Vertex<I, V, E, M> getVertex(I vertexIndex) {
    return vertexMap.get(vertexIndex);
  }

  @Override
  public Vertex<I, V, E, M> putVertex(Vertex<I, V, E, M> vertex) {
    return vertexMap.put(vertex.getId(), vertex);
  }

  @Override
  public Vertex<I, V, E, M> removeVertex(I vertexIndex) {
    return vertexMap.remove(vertexIndex);
  }

  @Override
  public void addPartition(Partition<I, V, E, M> partition) {
    for (Vertex<I, V, E , M> vertex : partition) {
      vertexMap.put(vertex.getId(), vertex);
    }
  }

  @Override
  public long getVertexCount() {
    return vertexMap.size();
  }

  @Override
  public long getEdgeCount() {
    long edges = 0;
    for (Vertex<I, V, E, M> vertex : vertexMap.values()) {
      edges += vertex.getNumEdges();
    }
    return edges;
  }

  @Override
  public void saveVertex(Vertex<I, V, E, M> vertex) {
    // No-op, vertices are stored as Java objects in this partition
  }

  @Override
  public String toString() {
    return "(id=" + getId() + ",V=" + vertexMap.size() + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
    int vertices = input.readInt();
    for (int i = 0; i < vertices; ++i) {
      progress();
      Vertex<I, V, E, M> vertex =
          WritableUtils.readVertexFromDataInput(input, getConf());
      if (vertexMap.put(vertex.getId(), vertex) != null) {
        throw new IllegalStateException(
            "readFields: " + this +
            " already has same id " + vertex);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeInt(vertexMap.size());
    for (Vertex<I, V, E, M> vertex : vertexMap.values()) {
      progress();
      WritableUtils.writeVertexToDataOutput(output, vertex, getConf());
    }
  }

  @Override
  public Iterator<Vertex<I, V, E, M>> iterator() {
    return vertexMap.values().iterator();
  }
}
