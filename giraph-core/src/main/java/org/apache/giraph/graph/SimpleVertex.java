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

import com.google.common.collect.Lists;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Vertex with no edge values.
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <M> Message data
 */
public abstract class SimpleVertex<I extends WritableComparable,
    V extends Writable, M extends Writable> extends Vertex<I, V,
    NullWritable, M> {
  /**
   * Set the neighbors of this vertex.
   *
   * @param neighbors Iterable of destination vertex ids.
   */
  public abstract void setNeighbors(Iterable<I> neighbors);

  @Override
  public void setEdges(Iterable<Edge<I, NullWritable>> edges) {
    setNeighbors(EdgeIterables.getNeighbors(edges));
  }

  /**
   * Get a read-only view of the neighbors of this
   * vertex, i.e. the target vertices of its out-edges.
   *
   * @return the neighbors (sort order determined by subclass implementation).
   */
  public abstract Iterable<I> getNeighbors();

  @Override
  public Iterable<Edge<I, NullWritable>> getEdges() {
    return EdgeIterables.getEdges(getNeighbors());
  }

  @Override
  public NullWritable getEdgeValue(I targetVertexId) {
    return NullWritable.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    I vertexId = getConf().createVertexId();
    vertexId.readFields(in);
    V vertexValue = getConf().createVertexValue();
    vertexValue.readFields(in);

    int numEdges = in.readInt();
    List<I> neighbors = Lists.newArrayListWithCapacity(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = getConf().createVertexId();
      targetVertexId.readFields(in);
      neighbors.add(targetVertexId);
    }

    initialize(vertexId, vertexValue);
    setNeighbors(neighbors);

    readHaltBoolean(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(getNumEdges());
    for (I neighbor : getNeighbors()) {
      neighbor.write(out);
    }

    out.writeBoolean(isHalted());
  }
}
