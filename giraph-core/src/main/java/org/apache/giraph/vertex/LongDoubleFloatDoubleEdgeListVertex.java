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

package org.apache.giraph.vertex;

import com.google.common.collect.Iterables;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.utils.UnmodifiableLongFloatEdgeArrayIterable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Compact vertex representation with primitive arrays.
 */
public abstract class LongDoubleFloatDoubleEdgeListVertex
    extends Vertex<LongWritable, DoubleWritable,
                   FloatWritable, DoubleWritable> {
  /** long represented vertex id */
  private long id;
  /** double represented vertex value */
  private double value;
  /** long array of neighbor vertex IDs */
  private long[] neighbors;
  /** float array of edge weights */
  private float[] edgeWeights;

  @Override
  public void initialize(LongWritable vertexId, DoubleWritable vertexValue) {
    id = vertexId.get();
    value = vertexValue.get();
  }

  @Override
  public void initialize(LongWritable vertexId, DoubleWritable vertexValue,
                         Iterable<Edge<LongWritable, FloatWritable>> edges) {
    id = vertexId.get();
    value = vertexValue.get();
    setEdges(edges);
  }

  @Override
  public void setEdges(Iterable<Edge<LongWritable, FloatWritable>> edges) {
    neighbors = new long[(edges != null) ? Iterables.size(edges) : 0];
    edgeWeights = new float[(edges != null) ? Iterables.size(edges) : 0];
    int n = 0;
    if (edges != null) {
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        neighbors[n] = edge.getTargetVertexId().get();
        edgeWeights[n] = edge.getValue().get();
        n++;
      }
    }
  }

  @Override
  public LongWritable getId() {
    return new LongWritable(id);
  }

  @Override
  public DoubleWritable getValue() {
    return new DoubleWritable(value);
  }

  @Override
  public void setValue(DoubleWritable vertexValue) {
    value = vertexValue.get();
  }

  @Override
  public Iterable<Edge<LongWritable, FloatWritable>> getEdges() {
    return new UnmodifiableLongFloatEdgeArrayIterable(neighbors, edgeWeights);
  }

  @Override
  public FloatWritable getEdgeValue(LongWritable targetVertexId) {
    int idx = 0;
    for (long neighbor : neighbors) {
      if (neighbor == targetVertexId.get()) {
        return new FloatWritable(edgeWeights[idx]);
      }
      idx++;
    }
    return null;
  }

  @Override
  public boolean hasEdge(LongWritable targetVertexId) {
    for (long neighbor : neighbors) {
      if (neighbor == targetVertexId.get()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getNumEdges() {
    return neighbors.length;
  }

  @Override
  public void sendMessageToAllEdges(final DoubleWritable message) {
    for (long neighbor : neighbors) {
      sendMessage(new LongWritable(neighbor), message);
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeDouble(value);
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeLong(neighbors[n]);
    }
    for (int n = 0; n < edgeWeights.length; n++) {
      out.writeFloat(edgeWeights[n]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    value = in.readDouble();
    int numEdges = in.readInt();
    neighbors = new long[numEdges];
    for (int n = 0; n < numEdges; n++) {
      neighbors[n] = in.readLong();
    }
    edgeWeights = new float[numEdges];
    for (int n = 0; n < numEdges; n++) {
      edgeWeights[n] = in.readFloat();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (id ^ (id >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof LongDoubleFloatDoubleEdgeListVertex)) {
      return false;
    }
    LongDoubleFloatDoubleEdgeListVertex other =
        (LongDoubleFloatDoubleEdgeListVertex) obj;
    if (id != other.id) {
      return false;
    }
    return true;
  }
}
