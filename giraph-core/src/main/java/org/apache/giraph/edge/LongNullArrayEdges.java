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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Implementation of {@link VertexEdges} with long ids and null edge
 * values, backed by a dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 */
public class LongNullArrayEdges
    extends ConfigurableVertexEdges<LongWritable, NullWritable>
    implements ReuseObjectsVertexEdges<LongWritable, NullWritable> {
  /** Array of target vertex ids. */
  private LongArrayList neighbors;

  @Override
  public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {
    // If the iterable is actually a collection, we can cheaply get the
    // size and initialize the arrays with the expected capacity.
    if (edges instanceof Collection) {
      int numEdges =
          ((Collection<Edge<LongWritable, NullWritable>>) edges).size();
      initialize(numEdges);
    } else {
      initialize();
    }
    for (Edge<LongWritable, NullWritable> edge : edges) {
      add(edge);
    }
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new LongArrayList(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new LongArrayList();
  }

  @Override
  public void add(Edge<LongWritable, NullWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
  }

  /**
   * If the backing array is more than four times as big as the number of
   * elements, halve its size.
   */
  private void trim() {
    if (neighbors.elements().length > 4 * neighbors.size()) {
      neighbors.trim(neighbors.elements().length / 2);
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
    } else {
      neighbors.set(i, neighbors.popLong());
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
  public Iterator<Edge<LongWritable, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<LongWritable, NullWritable>>() {
      /** Wrapped neighbors iterator. */
      private LongIterator neighborsIt = neighbors.iterator();
      /** Representative edge object. */
      private Edge<LongWritable, NullWritable> representativeEdge =
          getConf().createEdge();

      @Override
      public boolean hasNext() {
        return neighborsIt.hasNext();
      }

      @Override
      public Edge<LongWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(neighborsIt.nextLong());
        return representativeEdge;
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(neighbors.size());
    LongIterator neighborsIt = neighbors.iterator();
    while (neighborsIt.hasNext()) {
      out.writeLong(neighborsIt.nextLong());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      neighbors.add(in.readLong());
    }
  }
}

