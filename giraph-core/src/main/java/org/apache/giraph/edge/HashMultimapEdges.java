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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link OutEdges} implementation backed by an {@link ArrayListMultimap}.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for fast mutations,
 * but uses more space.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class HashMultimapEdges<I extends WritableComparable, E extends Writable>
    extends ConfigurableOutEdges<I, E>
    implements MultiRandomAccessOutEdges<I, E>, Trimmable {
  /** Multimap from target vertex id to edge values. */
  private ArrayListMultimap<I, E> edgeMultimap;

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  /**
   * Additional initialization method tailored to the underlying multimap
   * implementation.
   *
   * @param expectedNeighbors Expected number of unique neighbors
   * @param expectedEdgesPerNeighbor Expected number of edges per neighbor
   */
  public void initialize(int expectedNeighbors, int expectedEdgesPerNeighbor) {
    edgeMultimap = ArrayListMultimap.create(expectedNeighbors,
        expectedEdgesPerNeighbor);
  }

  @Override
  public void initialize(int capacity) {
    // To be conservative in terms of space usage, we assume that the initial
    // number of values per key is 1.
    initialize(capacity, 1);
  }

  @Override
  public void initialize() {
    edgeMultimap = ArrayListMultimap.create();
  }

  @Override
  public void add(Edge<I, E> edge) {
    edgeMultimap.put(edge.getTargetVertexId(), edge.getValue());
  }

  @Override
  public void remove(I targetVertexId) {
    edgeMultimap.removeAll(targetVertexId);
  }

  @Override
  public Iterable<E> getAllEdgeValues(I targetVertexId) {
    return edgeMultimap.get(targetVertexId);
  }

  @Override
  public int size() {
    return edgeMultimap.size();
  }

  @Override
  public Iterator<Edge<I, E>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<I, E>>() {
      /** Wrapped map iterator. */
      private Iterator<Map.Entry<I, E>> mapIterator =
          edgeMultimap.entries().iterator();
      /** Representative edge object. */
      private ReusableEdge<I, E> representativeEdge =
          getConf().createReusableEdge();

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
    // We write both the total number of edges and the number of unique
    // neighbors.
    out.writeInt(edgeMultimap.size());
    out.writeInt(edgeMultimap.keys().size());
    for (Map.Entry<I, E> edge : edgeMultimap.entries()) {
      edge.getKey().write(out);
      edge.getValue().write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // Given the total number of pairs and the number of unique neighbors,
    // we are able to compute the average number of edges per neighbors.
    int numEdges = in.readInt();
    int numNeighbors = in.readInt();
    initialize(numEdges, numNeighbors == 0 ? 0 : numEdges / numNeighbors);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = getConf().createVertexId();
      targetVertexId.readFields(in);
      E edgeValue = getConf().createEdgeValue();
      edgeValue.readFields(in);
      edgeMultimap.put(targetVertexId, edgeValue);
    }
  }

  @Override
  public void trim() {
    edgeMultimap.trimToSize();
  }
}
