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
package org.apache.giraph.edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Implementation of {@link OutEdges} with IDs and null edge values having
 * their TypeOps.
 * Backed by a dynamic primitive array. Parallel edges are allowed.
 * Note: this implementation is optimized for space
 * usage, but random access and edge removals are expensive.
 *
 * @param <I> Vertex id type
 */
public class IdAndNullArrayEdges<I extends WritableComparable>
  implements ReuseObjectsOutEdges<I, NullWritable>,
  MutableOutEdges<I, NullWritable>,
  ImmutableClassesGiraphConfigurable<I, Writable, NullWritable> {

  /** Array of target vertex ids. */
  private WArrayList<I> neighbors;

  @Override
  public
  ImmutableClassesGiraphConfiguration<I, Writable, NullWritable> getConf() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, Writable, NullWritable> conf) {
    PrimitiveIdTypeOps<I> idTypeOps =
        TypeOpsUtils.getPrimitiveIdTypeOps(conf.getVertexIdClass());
    neighbors = idTypeOps.createArrayList(10);
    if (!conf.getEdgeValueClass().equals(NullWritable.class)) {
      throw new IllegalArgumentException(
          "IdAndNullArrayEdges can be used only with NullWritable " +
          "as edgeValueClass, not with " + conf.getEdgeValueClass());
    }
  }

  @Override
  public void initialize(Iterable<Edge<I, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors.clear();
    neighbors.setCapacity(capacity);
  }

  @Override
  public void initialize() {
    initialize(10);
  }

  @Override
  public void add(Edge<I, NullWritable> edge) {
    neighbors.addW(edge.getTargetVertexId());
  }

  /**
   * If the backing array is more than four times as big as the number of
   * elements, reduce to 2 times current size.
   */
  private void trim() {
    if (neighbors.capacity() > 4 * neighbors.size()) {
      neighbors.setCapacity(neighbors.size() * 2);
    }
  }

  /**
   * Remove edge at position i.
   *
   * @param i Position of edge to be removed
   */
  private void removeAt(int i) {
    // The order of the edges is irrelevant, so we can simply replace
    // the deleted edge with the rightmost element, thus achieving constant
    // time.
    I tmpValue = neighbors.getElementTypeOps().create();
    neighbors.popIntoW(tmpValue);
    if (i != neighbors.size()) {
      neighbors.setW(i, tmpValue);
    }
    // If needed after the removal, trim the array.
    trim();
  }

  @Override
  public void remove(I targetVertexId) {
    // Thanks to the constant-time implementation of removeAt(int),
    // we can remove all matching edges in linear time.
    I tmpValue = neighbors.getElementTypeOps().create();
    for (int i = neighbors.size() - 1; i >= 0; --i) {
      neighbors.getIntoW(i, tmpValue);
      if (tmpValue.equals(targetVertexId)) {
        removeAt(i);
      }
    }
  }

  @Override
  public int size() {
    return neighbors.size();
  }

  @Override
  public Iterator<Edge<I, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    // The downcast is fine because all concrete Edge implementations are
    // mutable, but we only expose the mutation functionality when appropriate.
    return (Iterator) mutableIterator();
  }

  @Override
  public Iterator<MutableEdge<I, NullWritable>> mutableIterator() {
    return new Iterator<MutableEdge<I, NullWritable>>() {
      /** Current position in the array. */
      private int offset = 0;
      /** Representative edge object. */
      private final MutableEdge<I, NullWritable> representativeEdge =
          EdgeFactory.createReusable(neighbors.getElementTypeOps().create());

      @Override
      public boolean hasNext() {
        return offset < neighbors.size();
      }

      @Override
      public MutableEdge<I, NullWritable> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        neighbors.getIntoW(offset++, representativeEdge.getTargetVertexId());
        return representativeEdge;
      }

      @Override
      public void remove() {
        // Since removeAt() might replace the deleted edge with the last edge
        // in the array, we need to decrease the offset so that the latter
        // won't be skipped.
        removeAt(--offset);
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    neighbors.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    neighbors.readFields(in);
  }
}
