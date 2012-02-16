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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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
  private final int partitionId;
  /** Vertex map for this range (keyed by index) */
  private final Map<I, BasicVertex<I, V, E, M>> vertexMap =
      new HashMap<I, BasicVertex<I, V, E, M>>();

  /**
   * Constructor.
   *
   * @param conf Configuration.
   * @param partitionId Partition id.
   */
  public Partition(Configuration conf, int partitionId) {
    this.conf = conf;
    this.partitionId = partitionId;
  }

  /**
   * Get the vertex for this vertex index.
   *
   * @param vertexIndex Vertex index to search for
   * @return Vertex if it exists, null otherwise
   */
  public BasicVertex<I, V, E, M> getVertex(I vertexIndex) {
    return vertexMap.get(vertexIndex);
  }

  /**
   * Put a vertex into the Partition
   *
   * @param vertex Vertex to put in the Partition
   * @return old vertex value (i.e. null if none existed prior)
   */
  public BasicVertex<I, V, E, M> putVertex(BasicVertex<I, V, E, M> vertex) {
    return vertexMap.put(vertex.getVertexId(), vertex);
  }

  /**
   * Remove a vertex from the Partition
   *
   * @param vertexIndex Vertex index to remove
   * @return The removed vertex.
   */
  public BasicVertex<I, V, E, M> removeVertex(I vertexIndex) {
    return vertexMap.remove(vertexIndex);
  }

  /**
   * Get a collection of the vertices.
   *
   * @return Collection of the vertices
   */
  public Collection<BasicVertex<I, V, E , M>> getVertices() {
    return vertexMap.values();
  }

  /**
   * Get the number of edges in this partition.  Computed on the fly.
   *
   * @return Number of edges.
   */
  public long getEdgeCount() {
    long edges = 0;
    for (BasicVertex<I, V, E, M> vertex : vertexMap.values()) {
      edges += vertex.getNumOutEdges();
    }
    return edges;
  }

  /**
   * Get the partition id.
   *
   * @return Partition id of this partition.
   */
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public String toString() {
    return "(id=" + getPartitionId() + ",V=" + vertexMap.size() +
        ",E=" + getEdgeCount() + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int vertices = input.readInt();
    for (int i = 0; i < vertices; ++i) {
      BasicVertex<I, V, E, M> vertex =
        BspUtils.<I, V, E, M>createVertex(conf);
      vertex.readFields(input);
      if (vertexMap.put(vertex.getVertexId(),
          (BasicVertex<I, V, E, M>) vertex) != null) {
        throw new IllegalStateException(
            "readFields: " + this +
            " already has same id " + vertex);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(vertexMap.size());
    for (BasicVertex vertex : vertexMap.values()) {
      vertex.write(output);
    }
  }
}
