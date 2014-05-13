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

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * {@link OutEdges} implementation with long ids and null edge values,
 * backed by a {@link LongOpenHashSet}.
 * Parallel edges are not allowed.
 * Note: this implementation is optimized for fast random access and mutations,
 * and uses less space than a generic {@link HashMapEdges} (but more than
 * {@link LongNullArrayEdges}.
 */
public class LongNullHashSetEdges
    implements ReuseObjectsOutEdges<LongWritable, NullWritable>,
    MutableOutEdges<LongWritable, NullWritable>,
    StrictRandomAccessOutEdges<LongWritable, NullWritable>,
    Trimmable {
  /** Hash set of target vertex ids. */
  private LongOpenHashSet neighbors;

  @Override
  public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new LongOpenHashSet(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new LongOpenHashSet();
  }

  @Override
  public void add(Edge<LongWritable, NullWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    neighbors.remove(targetVertexId.get());
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
      /** Wrapped neighbors iterator. */
      private LongIterator neighborsIt = neighbors.iterator();
      /** Representative edge object. */
      private ReusableEdge<LongWritable, NullWritable> representativeEdge =
          EdgeFactory.createReusable(new LongWritable());

      public boolean hasNext() {
        return neighborsIt.hasNext();
      }

      @Override
      public MutableEdge<LongWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(neighborsIt.nextLong());
        return representativeEdge;
      }

      @Override
      public void remove() {
        neighborsIt.remove();
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
  public NullWritable getEdgeValue(LongWritable targetVertexId) {
    if (neighbors.contains(targetVertexId.get())) {
      return NullWritable.get();
    } else {
      return null;
    }
  }

  @Override
  public void setEdgeValue(LongWritable targetVertexId,
    NullWritable edgeValue) {
    // No operation.
    // Only set value for an existing edge.
    // If the edge exist, the Null value is already there.
  }

  @Override
  public void trim() {
    neighbors.trim();
  }
}
