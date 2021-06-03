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

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.UnmodifiableIterator;

/**
 * {@link OutEdges} implementation with long ids and byte edge values,
 * backed by a {@link Long2ByteOpenHashMap}.
 * Parallel edges are not allowed.
 * Note: this implementation is optimized for fast random access and mutations,
 * and uses less space than a generic {@link HashMapEdges}.
 */
public class LongByteHashMapEdges
    implements StrictRandomAccessOutEdges<LongWritable, ByteWritable>,
    ReuseObjectsOutEdges<LongWritable, ByteWritable>,
    MutableOutEdges<LongWritable, ByteWritable>, Trimmable {
  /** Hash map from target vertex id to edge value. */
  private Long2ByteOpenHashMap edgeMap;
  /** Representative edge value object, used by getEdgeValue(). */
  private ByteWritable representativeEdgeValue;

  @Override
  public void initialize(Iterable<Edge<LongWritable, ByteWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    edgeMap = new Long2ByteOpenHashMap(capacity);
  }

  @Override
  public void initialize() {
    edgeMap = new Long2ByteOpenHashMap();
  }

  @Override
  public void add(Edge<LongWritable, ByteWritable> edge) {
    edgeMap.put(edge.getTargetVertexId().get(), edge.getValue().get());
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    edgeMap.remove(targetVertexId.get());
  }

  @Override
  public ByteWritable getEdgeValue(LongWritable targetVertexId) {
    if (!edgeMap.containsKey(targetVertexId.get())) {
      return null;
    }
    if (representativeEdgeValue == null) {
      representativeEdgeValue = new ByteWritable();
    }
    representativeEdgeValue.set(edgeMap.get(targetVertexId.get()));
    return representativeEdgeValue;
  }

  @Override
  public void setEdgeValue(LongWritable targetVertexId,
                           ByteWritable edgeValue) {
    if (edgeMap.containsKey(targetVertexId.get())) {
      edgeMap.put(targetVertexId.get(), edgeValue.get());
    }
  }

  @Override
  public int size() {
    return edgeMap.size();
  }

  @Override
  public Iterator<Edge<LongWritable, ByteWritable>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<LongWritable, ByteWritable>>() {
      /** Wrapped map iterator. */
      private final ObjectIterator<Long2ByteMap.Entry> mapIterator =
          edgeMap.long2ByteEntrySet().fastIterator();
      /** Representative edge object. */
      private final ReusableEdge<LongWritable, ByteWritable>
      representativeEdge =
          EdgeFactory.createReusable(new LongWritable(), new ByteWritable());

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public Edge<LongWritable, ByteWritable> next() {
        Long2ByteMap.Entry nextEntry = mapIterator.next();
        representativeEdge.getTargetVertexId().set(nextEntry.getLongKey());
        representativeEdge.getValue().set(nextEntry.getByteValue());
        return representativeEdge;
      }
    };
  }

  @Override
  public void trim() {
    edgeMap.trim();
  }

  /** Helper class for a mutable edge that modifies the backing map entry. */
  private static class LongByteHashMapMutableEdge
      extends DefaultEdge<LongWritable, ByteWritable> {
    /** Backing entry for the edge in the map. */
    private Long2ByteMap.Entry entry;

    /** Constructor. */
    public LongByteHashMapMutableEdge() {
      super(new LongWritable(), new ByteWritable());
    }

    /**
     * Make the edge point to the given entry in the backing map.
     *
     * @param entry Backing entry
     */
    public void setEntry(Long2ByteMap.Entry entry) {
      // Update the id and value objects from the superclass.
      getTargetVertexId().set(entry.getLongKey());
      getValue().set(entry.getByteValue());
      // Update the entry.
      this.entry = entry;
    }

    @Override
    public void setValue(ByteWritable value) {
      // Update the value object from the superclass.
      getValue().set(value.get());
      // Update the value stored in the backing map.
      entry.setValue(value.get());
    }
  }

  @Override
  public Iterator<MutableEdge<LongWritable, ByteWritable>> mutableIterator() {
    return new Iterator<MutableEdge<LongWritable, ByteWritable>>() {
      /**
       * Wrapped map iterator.
       * Note: we cannot use the fast iterator in this case,
       * because we need to call setValue() on an entry.
       */
      private final ObjectIterator<Long2ByteMap.Entry> mapIterator =
          edgeMap.long2ByteEntrySet().iterator();
      /** Representative edge object. */
      private final LongByteHashMapMutableEdge representativeEdge =
          new LongByteHashMapMutableEdge();

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public MutableEdge<LongWritable, ByteWritable> next() {
        representativeEdge.setEntry(mapIterator.next());
        return representativeEdge;
      }

      @Override
      public void remove() {
        mapIterator.remove();
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(edgeMap.size());
    for (Long2ByteMap.Entry entry : edgeMap.long2ByteEntrySet()) {
      out.writeLong(entry.getLongKey());
      out.writeByte(entry.getByteValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      edgeMap.put(in.readLong(), in.readByte());
    }
  }
}
