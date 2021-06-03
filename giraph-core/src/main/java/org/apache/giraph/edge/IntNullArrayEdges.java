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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.UnmodifiableIterator;

/**
 * Implementation of {@link OutEdges} with int ids and null edge
 * values, backed by dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 */
public class IntNullArrayEdges
    implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {
  /** Array of target vertex ids */
  private IntArrayList neighbors;

  @Override
  public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new IntArrayList(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new IntArrayList();
  }

  @Override
  public int size() {
    return neighbors.size();
  }

  @Override
  public void add(Edge<IntWritable, NullWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
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
      neighbors.popInt();
    } else {
      neighbors.set(i, neighbors.popInt());
    }
  }

  @Override
  public void remove(IntWritable targetVertexId) {
    // Thanks to the constant-time implementation of removeAt(int),
    // we can remove all matching edges in linear time.
    for (int i = neighbors.size() - 1; i >= 0; --i) {
      if (neighbors.getInt(i) == targetVertexId.get()) {
        removeAt(i);
      }
    }
  }

  @Override
  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<IntWritable, NullWritable>>() {
      /** Wrapped neighbors iterator. */
      private final IntIterator neighborsIt = neighbors.iterator();
      /** Representative edge object. */
      private final Edge<IntWritable, NullWritable> representativeEdge =
          EdgeFactory.create(new IntWritable(), NullWritable.get());

      @Override
      public boolean hasNext() {
        return neighborsIt.hasNext();
      }

      @Override
      public Edge<IntWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(neighborsIt.nextInt());
        return representativeEdge;
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(neighbors.size());
    IntIterator iterator = neighbors.iterator();
    while (iterator.hasNext()) {
      out.writeInt(iterator.nextInt());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      neighbors.add(in.readInt());
    }
  }

  @Override
  public void trim() {
    neighbors.trim();
  }
}
