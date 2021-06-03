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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of {@link OutEdges} with long ids and null edge
 * values, backed by a dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 */
public class LongNullArrayEdges
    implements ReuseObjectsOutEdges<LongWritable, NullWritable>,
    MutableOutEdges<LongWritable, NullWritable>, Trimmable {
  /** Array of target vertex ids. */
  private LongArrayList neighbors;

  @Override
  public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
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
  private void trimBack() {
    if (neighbors.elements().length > 4 * neighbors.size()) {
      neighbors.trim(neighbors.elements().length / 2);
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
    } else {
      neighbors.set(i, neighbors.popLong());
    }
    // If needed after the removal, trim the array.
    trimBack();
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    // Thanks to the constant-time implementation of removeAt(int),
    // we can remove all matching edges in linear time.
    for (int i = neighbors.size() - 1; i >= 0; --i) {
      if (neighbors.getLong(i) == targetVertexId.get()) {
        removeAt(i);
      }
    }
  }

  @Override
  public int size() {
    return neighbors.size();
  }

  @Override
  public Iterator<Edge<LongWritable, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    // The downcast is fine because all concrete Edge implementations are
    // mutable, but we only expose the mutation functionality when appropriate.
    return (Iterator) mutableIterator();
  }

  @Override
  public Iterator<MutableEdge<LongWritable, NullWritable>> mutableIterator() {
    return new Iterator<MutableEdge<LongWritable, NullWritable>>() {
      /** Current position in the array. */
      private int offset = 0;
      /** Representative edge object. */
      private final MutableEdge<LongWritable, NullWritable> representativeEdge =
          EdgeFactory.createReusable(new LongWritable());

      @Override
      public boolean hasNext() {
        return offset < neighbors.size();
      }

      @Override
      public MutableEdge<LongWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(neighbors.getLong(offset++));
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

  @Override
  public void trim() {
    neighbors.trim();
  }
}

