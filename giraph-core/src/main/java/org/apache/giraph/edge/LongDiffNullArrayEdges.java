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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of {@link org.apache.giraph.edge.OutEdges} with long ids
 * and null edge values, backed by a dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 * Users of this class should explicitly call {@link #trim()} function
 * to compact in-memory representation after all updates are done.
 * Compacting object is expensive so should only be done once after bulk update.
 * Compaction can also be caused by serialization attempt or
 * by calling {@link #iterator()}
 */
@NotThreadSafe
public class LongDiffNullArrayEdges
    extends ConfigurableOutEdges<LongWritable, NullWritable>
    implements ReuseObjectsOutEdges<LongWritable, NullWritable>,
    MutableOutEdges<LongWritable, NullWritable>, Trimmable {

  /**
   * Compressed array of target vertex ids.
   */
  private LongDiffArray edges = new LongDiffArray();

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration
                      <LongWritable, Writable, NullWritable> conf) {
    super.setConf(conf);
    edges.setUseUnsafeSerialization(conf.getUseUnsafeSerialization());
  }

  @Override
  public void initialize(
    Iterable<Edge<LongWritable, NullWritable>> edgeIterator
  ) {
    edges.initialize();
    EdgeIterables.initialize(this, edgeIterator);
    edges.trim();
  }

  @Override
  public void initialize(int capacity) {
    edges.initialize(capacity);
  }

  @Override
  public void initialize() {
    edges.initialize();
  }

  @Override
  public void add(Edge<LongWritable, NullWritable> edge) {
    edges.add(edge.getTargetVertexId().get());
  }


  @Override
  public void remove(LongWritable targetVertexId) {
    edges.remove(targetVertexId.get());
  }

  @Override
  public int size() {
    return edges.size();
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
    trim();
    return new Iterator<MutableEdge<LongWritable, NullWritable>>() {
      private final Iterator<LongWritable> reader = edges.iterator();

      /** Representative edge object. */
      private final MutableEdge<LongWritable, NullWritable> representativeEdge =
          EdgeFactory.createReusable(new LongWritable());

      @Override
      public boolean hasNext() {
        return reader.hasNext();
      }

      @Override
      public MutableEdge<LongWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(reader.next().get());
        return representativeEdge;
      }

      @Override
      public void remove() {
        reader.remove();
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    edges.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    edges.readFields(in);
  }

  /**
   * This function takes all recent updates and stores them efficiently.
   * It is safe to call this function multiple times.
   */
  @Override
  public void trim() {
    edges.trim();
  }
}
