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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A complete edge, the destination vertex and the edge value.  Can only be one
 * edge with a destination vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class Edge<I extends WritableComparable, E extends Writable>
    implements WritableComparable<Edge<I, E>>, Configurable {
  /** Destination vertex id */
  private I destVertexId = null;
  /** Edge value */
  private E edgeValue = null;
  /** Configuration - Used to instantiate classes */
  private Configuration conf = null;

  /**
   * Constructor for reflection
   */
  public Edge() { }

  /**
   * Create the edge with final values
   *
   * @param destVertexId Desination vertex id.
   * @param edgeValue Value of the edge.
   */
  public Edge(I destVertexId, E edgeValue) {
    this.destVertexId = destVertexId;
    this.edgeValue = edgeValue;
  }

  /**
   * Get the destination vertex index of this edge
   *
   * @return Destination vertex index of this edge
   */
  public I getDestVertexId() {
    return destVertexId;
  }

  /**
   * Get the edge value of the edge
   *
   * @return Edge value of this edge
   */
  public E getEdgeValue() {
    return edgeValue;
  }

  /**
   * Set the destination vertex index of this edge.
   *
   * @param destVertexId new destination vertex
   */
  public void setDestVertexId(I destVertexId) {
    this.destVertexId = destVertexId;
  }

  /**
   * Set the value for this edge.
   *
   * @param edgeValue new edge value
   */
  public void setEdgeValue(E edgeValue) {
    this.edgeValue = edgeValue;
  }

  @Override
  public String toString() {
    return "(DestVertexIndex = " + destVertexId +
        ", edgeValue = " + edgeValue  + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    destVertexId = BspUtils.<I>createVertexIndex(getConf());
    destVertexId.readFields(input);
    edgeValue = BspUtils.<E>createEdgeValue(getConf());
    edgeValue.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    if (destVertexId == null) {
      throw new IllegalStateException(
          "write: Null destination vertex index");
    }
    if (edgeValue == null) {
      throw new IllegalStateException(
          "write: Null edge value");
    }
    destVertexId.write(output);
    edgeValue.write(output);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Edge<I, E> edge) {
    return destVertexId.compareTo(edge.getDestVertexId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Edge edge = (Edge) o;

    if (destVertexId != null ? !destVertexId.equals(edge.destVertexId) :
      edge.destVertexId != null) {
      return false;
    }
    if (edgeValue != null ?
        !edgeValue.equals(edge.edgeValue) : edge.edgeValue != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = destVertexId != null ? destVertexId.hashCode() : 0;
    result = 31 * result + (edgeValue != null ? edgeValue.hashCode() : 0);
    return result;
  }
}
