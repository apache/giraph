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

import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link VertexEdges} implementation backed by a {@link HashMap}.
 * Parallel edges are not allowed.
 * Note: this implementation is optimized for fast random access and mutations,
 * but uses more space.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class HashMapEdges<I extends WritableComparable, E extends Writable>
    extends ConfigurableVertexEdges<I, E>
    implements StrictRandomAccessVertexEdges<I, E> {
  /** Map from target vertex id to edge value. */
  private HashMap<I, E> edgeMap;

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    // If the iterable is actually a collection, we can cheaply get the
    // size and initialize the hash-map with the expected capacity.
    if (edges instanceof Collection) {
      initialize(((Collection<Edge<I, E>>) edges).size());
    } else {
      initialize();
    }
    for (Edge<I, E> edge : edges) {
      add(edge);
    }
  }

  @Override
  public void initialize(int capacity) {
    edgeMap = Maps.newHashMapWithExpectedSize(capacity);
  }

  @Override
  public void initialize() {
    edgeMap = Maps.newHashMap();
  }

  @Override
  public void add(Edge<I, E> edge) {
    edgeMap.put(edge.getTargetVertexId(), edge.getValue());
  }

  @Override
  public void remove(I targetVertexId) {
    edgeMap.remove(targetVertexId);
  }

  @Override
  public E getEdgeValue(I targetVertexId) {
    return edgeMap.get(targetVertexId);
  }

  @Override
  public int size() {
    return edgeMap.size();
  }

  @Override
  public Iterator<Edge<I, E>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<I, E>>() {
      /** Wrapped map iterator. */
      private Iterator<Map.Entry<I, E>> mapIterator =
          edgeMap.entrySet().iterator();
      /** Representative edge object. */
      private MutableEdge<I, E> representativeEdge =
          getConf().createMutableEdge();

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public Edge<I, E> next() {
        Map.Entry<I, E> nextEntry = mapIterator.next();
        representativeEdge.setTargetVertexId(nextEntry.getKey());
        representativeEdge.setValue(nextEntry.getValue());
        return representativeEdge;
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(edgeMap.size());
    for (Map.Entry<I, E> entry : edgeMap.entrySet()) {
      entry.getKey().write(out);
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = getConf().createVertexId();
      targetVertexId.readFields(in);
      E edgeValue = getConf().createEdgeValue();
      edgeValue.readFields(in);
      edgeMap.put(targetVertexId, edgeValue);
    }
  }
}
