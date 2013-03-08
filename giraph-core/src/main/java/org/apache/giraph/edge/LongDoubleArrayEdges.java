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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Implementation of {@link VertexEdges} with long ids and double edge
 * values, backed by dynamic primitive arrays.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 */
public class LongDoubleArrayEdges
    extends ConfigurableVertexEdges<LongWritable, DoubleWritable>
    implements ReuseObjectsVertexEdges<LongWritable, DoubleWritable> {
  /** Array of target vertex ids. */
  private LongArrayList neighbors;
  /** Array of edge values. */
  private DoubleArrayList edgeValues;

  @Override
  public void initialize(Iterable<Edge<LongWritable, DoubleWritable>> edges) {
    // If the iterable is actually a collection, we can cheaply get the
    // size and initialize the arrays with the expected capacity.
    if (edges instanceof Collection) {
      int numEdges =
          ((Collection<Edge<LongWritable, DoubleWritable>>) edges).size();
      initialize(numEdges);
    } else {
      initialize();
    }
    for (Edge<LongWritable, DoubleWritable> edge : edges) {
      add(edge);
    }
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
  private void remove(int i) {
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
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    // Thanks to the constant-time implementation of remove(int),
    // we can remove all matching edges in linear time.
    for (int i = neighbors.size() - 1; i >= 0; --i) {
      if (neighbors.get(i) == targetVertexId.get()) {
        remove(i);
      }
    }
    trim();
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
          getConf().createEdge();

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
