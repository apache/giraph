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

import com.google.common.collect.UnmodifiableIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Implementation of {@link OutEdges} with long ids and double edge
 * values, backed by dynamic primitive arrays.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 */
public class LongDoubleArrayEdges
    implements ReuseObjectsOutEdges<LongWritable, DoubleWritable>,
    MutableOutEdges<LongWritable, DoubleWritable> {
  /** Array of target vertex ids. */
  private LongArrayList neighbors;
  /** Array of edge values. */
  private DoubleArrayList edgeValues;

  @Override
  public void initialize(Iterable<Edge<LongWritable, DoubleWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new LongArrayList(capacity);
    edgeValues = new DoubleArrayList(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new LongArrayList();
    edgeValues = new DoubleArrayList();
  }

  @Override
  public void add(Edge<LongWritable, DoubleWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
    edgeValues.add(edge.getValue().get());
  }

  /**
   * If the backing arrays are more than four times as big as the number of
   * elements, halve their size.
   */
  private void trim() {
    if (neighbors.elements().length > 4 * neighbors.size()) {
      neighbors.trim(neighbors.elements().length / 2);
      edgeValues.trim(neighbors.elements().length / 2);
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
    if (i == neighbors.size() - 1) {
      neighbors.popLong();
      edgeValues.popDouble();
    } else {
      neighbors.set(i, neighbors.popLong());
      edgeValues.set(i, edgeValues.popDouble());
    }
    // If needed after the removal, trim the arrays.
    trim();
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    // Thanks to the constant-time implementation of removeAt(int),
    // we can remove all matching edges in linear time.
    for (int i = neighbors.size() - 1; i >= 0; --i) {
      if (neighbors.get(i) == targetVertexId.get()) {
        removeAt(i);
      }
    }
  }

  @Override
  public int size() {
    return neighbors.size();
  }

  @Override
  public Iterator<Edge<LongWritable, DoubleWritable>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<LongWritable, DoubleWritable>>() {
      /** Wrapped neighbors iterator. */
      private LongIterator neighborsIt = neighbors.iterator();
      /** Wrapped edge values iterator. */
      private DoubleIterator edgeValuesIt = edgeValues.iterator();
      /** Representative edge object. */
      private Edge<LongWritable, DoubleWritable> representativeEdge =
          EdgeFactory.create(new LongWritable(), new DoubleWritable());

      @Override
      public boolean hasNext() {
        return neighborsIt.hasNext();
      }

      @Override
      public Edge<LongWritable, DoubleWritable> next() {
        representativeEdge.getTargetVertexId().set(neighborsIt.nextLong());
        representativeEdge.getValue().set(edgeValuesIt.nextDouble());
        return representativeEdge;
      }
    };
  }

  /** Helper class for a mutable edge that modifies the backing arrays. */
  private class LongDoubleArrayMutableEdge
      extends DefaultEdge<LongWritable, DoubleWritable> {
    /** Index of the edge in the backing arrays. */
    private int index;

    /** Constructor. */
    public LongDoubleArrayMutableEdge() {
      super(new LongWritable(), new DoubleWritable());
    }

    /**
     * Make the edge point to the given index in the backing arrays.
     *
     * @param index Index in the arrays
     */
    public void setIndex(int index) {
      // Update the id and value objects from the superclass.
      getTargetVertexId().set(neighbors.get(index));
      getValue().set(edgeValues.get(index));
      // Update the index.
      this.index = index;
    }

    @Override
    public void setValue(DoubleWritable value) {
      // Update the value object from the superclass.
      getValue().set(value.get());
      // Update the value stored in the backing array.
      edgeValues.set(index, value.get());
    }
  }

  @Override
  public Iterator<MutableEdge<LongWritable, DoubleWritable>>
  mutableIterator() {
    return new Iterator<MutableEdge<LongWritable, DoubleWritable>>() {
      /** Current position in the array. */
      private int offset = 0;
      /** Representative edge object. */
      private LongDoubleArrayMutableEdge representativeEdge =
          new LongDoubleArrayMutableEdge();

      @Override
      public boolean hasNext() {
        return offset < neighbors.size();
      }

      @Override
      public MutableEdge<LongWritable, DoubleWritable> next() {
        representativeEdge.setIndex(offset++);
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
    out.writeInt(neighbors.size());
    LongIterator neighborsIt = neighbors.iterator();
    DoubleIterator edgeValuesIt = edgeValues.iterator();
    while (neighborsIt.hasNext()) {
      out.writeLong(neighborsIt.nextLong());
      out.writeDouble(edgeValuesIt.nextDouble());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      neighbors.add(in.readLong());
      edgeValues.add(in.readDouble());
    }
  }
}
