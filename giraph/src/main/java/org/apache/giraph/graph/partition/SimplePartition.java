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

package org.apache.giraph.graph.partition;

import com.google.common.collect.Maps;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

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
    implements Partition<I, V, E, M> {
  /** Configuration from the worker */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Partition id */
  private int id;
  /** Vertex map for this range (keyed by index) */
  private ConcurrentMap<I, Vertex<I, V, E, M>> vertexMap;
  /** Context used to report progress */
  private Progressable progressable;

  /**
   * Constructor for reflection.
   */
  public SimplePartition() { }

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    setId(partitionId);
    setProgressable(progressable);
    if (conf.getBoolean(GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES,
        GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES_DEFAULT)) {
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
  public int getId() {
    return id;
  }

  @Override
  public void setId(int id) {
    this.id = id;
  }

  @Override
  public void setProgressable(Progressable progressable) {
    this.progressable = progressable;
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
    if (conf.getBoolean(GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES,
        GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES_DEFAULT)) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
    id = input.readInt();
    int vertices = input.readInt();
    for (int i = 0; i < vertices; ++i) {
      Vertex<I, V, E, M> vertex = conf.createVertex();
      if (progressable != null) {
        progressable.progress();
      }
      vertex.readFields(input);
      if (vertexMap.put(vertex.getId(), vertex) != null) {
        throw new IllegalStateException(
            "readFields: " + this +
            " already has same id " + vertex);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(id);
    output.writeInt(vertexMap.size());
    for (Vertex vertex : vertexMap.values()) {
      if (progressable != null) {
        progressable.progress();
      }
      vertex.write(output);
    }
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration) {
    this.conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, V, E, M> getConf() {
    return conf;
  }

  @Override
  public Iterator<Vertex<I, V, E, M>> iterator() {
    return vertexMap.values().iterator();
  }
}
