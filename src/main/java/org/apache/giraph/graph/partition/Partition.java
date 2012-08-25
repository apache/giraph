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

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A generic container that stores vertices.  Vertex ids will map to exactly
 * one partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class Partition<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements Writable {
  /** Configuration from the worker */
  private final Configuration conf;
  /** Partition id */
  private final int id;
  /** Vertex map for this range (keyed by index) */
  private final ConcurrentMap<I, Vertex<I, V, E, M>> vertexMap;

  /**
   * Constructor.
   *
   * @param conf Configuration.
   * @param id Partition id.
   */
  public Partition(Configuration conf, int id) {
    this.conf = conf;
    this.id = id;
    if (conf.getBoolean(GiraphJob.USE_OUT_OF_CORE_MESSAGES,
        GiraphJob.USE_OUT_OF_CORE_MESSAGES_DEFAULT)) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
  }

  /**
   * Get the vertex for this vertex index.
   *
   * @param vertexIndex Vertex index to search for
   * @return Vertex if it exists, null otherwise
   */
  public Vertex<I, V, E, M> getVertex(I vertexIndex) {
    return vertexMap.get(vertexIndex);
  }

  /**
   * Put a vertex into the Partition
   *
   * @param vertex Vertex to put in the Partition
   * @return old vertex value (i.e. null if none existed prior)
   */
  public Vertex<I, V, E, M> putVertex(Vertex<I, V, E, M> vertex) {
    return vertexMap.put(vertex.getId(), vertex);
  }

  /**
   * Remove a vertex from the Partition
   *
   * @param vertexIndex Vertex index to remove
   * @return The removed vertex.
   */
  public Vertex<I, V, E, M> removeVertex(I vertexIndex) {
    return vertexMap.remove(vertexIndex);
  }

  /**
   * Get a collection of the vertices.
   *
   * @return Collection of the vertices
   */
  public Collection<Vertex<I, V, E , M>> getVertices() {
    return vertexMap.values();
  }

  /**
   * Put several vertices in the partition.
   *
   * @param vertices Vertices to add
   */
  public void putVertices(Collection<Vertex<I, V, E , M>> vertices) {
    for (Vertex<I, V, E , M> vertex : vertices) {
      vertexMap.put(vertex.getId(), vertex);
    }
  }

  /**
   * Get the number of edges in this partition.  Computed on the fly.
   *
   * @return Number of edges.
   */
  public long getEdgeCount() {
    long edges = 0;
    for (Vertex<I, V, E, M> vertex : vertexMap.values()) {
      edges += vertex.getNumEdges();
    }
    return edges;
  }

  /**
   * Get the partition id.
   *
   * @return Id of this partition.
   */
  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return "(id=" + getId() + ",V=" + vertexMap.size() +
        ",E=" + getEdgeCount() + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int vertices = input.readInt();
    for (int i = 0; i < vertices; ++i) {
      Vertex<I, V, E, M> vertex =
        BspUtils.<I, V, E, M>createVertex(conf);
      vertex.readFields(input);
      if (vertexMap.put(vertex.getId(),
          (Vertex<I, V, E, M>) vertex) != null) {
        throw new IllegalStateException(
            "readFields: " + this +
            " already has same id " + vertex);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(vertexMap.size());
    for (Vertex vertex : vertexMap.values()) {
      vertex.write(output);
    }
  }
}
