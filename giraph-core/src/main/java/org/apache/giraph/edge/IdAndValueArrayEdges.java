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
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.UnmodifiableIterator;

/**
 * Implementation of {@link OutEdges} with IDs and Edge values having their
 * TypeOps.
 * Data is backed by a dynamic primitive array. Parallel edges are allowed.
 * Note: this implementation is optimized for space usage, but random access
 * and edge removals are expensive.
 *
 * @param <I> Vertex id type
 * @param <E> Edge value type
 */
public class IdAndValueArrayEdges<I extends WritableComparable,
    E extends Writable> implements ReuseObjectsOutEdges<I, E>,
    MutableOutEdges<I, E>,
    ImmutableClassesGiraphConfigurable<I, Writable, E> {

  /** Array of target vertex ids. */
  private WArrayList<I> neighborIds;
  /** Array of edge values. */
  private WArrayList<E> neighborEdgeValues;

  @Override
  public ImmutableClassesGiraphConfiguration<I, Writable, E> getConf() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    PrimitiveIdTypeOps<I> idTypeOps =
        TypeOpsUtils.getPrimitiveIdTypeOps(conf.getVertexIdClass());
    neighborIds = idTypeOps.createArrayList(10);

    PrimitiveTypeOps<E> edgeTypeOps =
        TypeOpsUtils.getPrimitiveTypeOps(conf.getEdgeValueClass());
    neighborEdgeValues = edgeTypeOps.createArrayList(10);
  }

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighborIds.clear();
    neighborIds.setCapacity(capacity);
    neighborEdgeValues.clear();
    neighborEdgeValues.setCapacity(capacity);
  }

  @Override
  public void initialize() {
    initialize(10);
  }

  @Override
  public void add(Edge<I, E> edge) {
    neighborIds.addW(edge.getTargetVertexId());
    neighborEdgeValues.addW(edge.getValue());
  }

  /**
   * If the backing array is more than four times as big as the number of
   * elements, reduce to 2 times current size.
   */
  private void trim() {
    if (neighborIds.capacity() > 4 * neighborIds.size()) {
      neighborIds.setCapacity(neighborIds.size() * 2);
      neighborEdgeValues.setCapacity(neighborIds.size() * 2);
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
    I tmpId = neighborIds.getElementTypeOps().create();
    E tmpValue = neighborEdgeValues.getElementTypeOps().create();

    neighborIds.popIntoW(tmpId);
    neighborEdgeValues.popIntoW(tmpValue);
    if (i != neighborIds.size()) {
      neighborIds.setW(i, tmpId);
      neighborEdgeValues.setW(i, tmpValue);
    }
    // If needed after the removal, trim the array.
    trim();
  }

  @Override
  public void remove(I targetVertexId) {
    // Thanks to the constant-time implementation of removeAt(int),
    // we can remove all matching edges in linear time.
    I tmpId = neighborIds.getElementTypeOps().create();
    for (int i = neighborIds.size() - 1; i >= 0; --i) {
      neighborIds.getIntoW(i, tmpId);
      if (tmpId.equals(targetVertexId)) {
        removeAt(i);
      }
    }
  }

  @Override
  public int size() {
    return neighborIds.size();
  }

  @Override
  public Iterator<Edge<I, E>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<I, E>>() {
      private int index;

      /** Representative edge object. */
      private final Edge<I, E> representativeEdge = EdgeFactory.create(
          neighborIds.getElementTypeOps().create(),
          neighborEdgeValues.getElementTypeOps().create());

      @Override
      public boolean hasNext() {
        return index < neighborIds.size();
      }

      @Override
      public Edge<I, E> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        neighborIds.getIntoW(index, representativeEdge.getTargetVertexId());
        neighborEdgeValues.getIntoW(index, representativeEdge.getValue());
        index++;
        return representativeEdge;
      }
    };
  }

  /** Helper class for a mutable edge that modifies the backing arrays. */
  private class ArrayMutableEdge extends DefaultEdge<I, E> {
    /** Index of the edge in the backing arrays. */
    private int index;

    /** Constructor. */
    public ArrayMutableEdge() {
      super(
          neighborIds.getElementTypeOps().create(),
          neighborEdgeValues.getElementTypeOps().create());
    }

    /**
     * Make the edge point to the given index in the backing arrays.
     *
     * @param index Index in the arrays
     */
    public void setIndex(int index) {
      // Update the id and value objects from the superclass.
      neighborIds.getIntoW(index, getTargetVertexId());
      neighborEdgeValues.getIntoW(index, getValue());
      // Update the index.
      this.index = index;
    }

    @Override
    public void setValue(E value) {
      // Update the value object from the superclass.
      neighborEdgeValues.getElementTypeOps().set(getValue(), value);
      // Update the value stored in the backing array.
      neighborEdgeValues.setW(index, value);
    }
  }

  @Override
  public Iterator<MutableEdge<I, E>> mutableIterator() {
    return new Iterator<MutableEdge<I, E>>() {
      /** Current position in the array. */
      private int index = 0;
      /** Representative edge object. */
      private final ArrayMutableEdge representativeEdge =
          new ArrayMutableEdge();

      @Override
      public boolean hasNext() {
        return index < neighborIds.size();
      }

      @Override
      public MutableEdge<I, E> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        representativeEdge.setIndex(index++);
        return representativeEdge;
      }

      @Override
      public void remove() {
        // Since removeAt() might replace the deleted edge with the last edge
        // in the array, we need to decrease the offset so that the latter
        // won't be skipped.
        removeAt(--index);
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    neighborIds.write(out);
    neighborEdgeValues.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    neighborIds.readFields(in);
    neighborEdgeValues.readFields(in);
  }
}
