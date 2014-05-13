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

package org.apache.giraph.utils;

import com.google.common.collect.Iterables;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Utility methods for iterables of edges.
 */
public class EdgeIterables {
  /** Utility classes shouldn't be instantiated. */
  private EdgeIterables() { }

  /**
   * Compare two edge iterables element-wise. The edge value type needs
   * to be Comparable.
   *
   * @param e1 First edge iterable
   * @param e2 Second edge iterable
   * @param <I> Vertex id
   * @param <E> Edge value
   * @return Whether the two iterables are equal element-wise
   */
  public static <I extends WritableComparable, E extends WritableComparable>
  boolean equals(Iterable<Edge<I, E>> e1, Iterable<Edge<I, E>> e2) {
    Iterator<Edge<I, E>> i1 = e1.iterator();
    Iterator<Edge<I, E>> i2 = e2.iterator();
    while (i1.hasNext()) {
      if (!i2.hasNext()) {
        return false;
      }
      if (!EdgeComparator.equal(i1.next(), i2.next())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Make a deep copy of an edge iterable and return it as an {@link
   * ArrayList}.
   * Note: this method is slow since it has to deserialize all serialize all
   * the ids and values. It should only be used in unit tests.
   *
   * @param edges Iterable of edges
   * @param <I> Vertex id
   * @param <E> Edge value
   * @return A new list with copies of all the edges
   */
  public static <I extends WritableComparable, E extends WritableComparable>
  ArrayList<Edge<I, E>> copy(Iterable<Edge<I, E>> edges) {
    Configuration conf = new Configuration();
    ArrayList<Edge<I, E>> edgeList =
        new ArrayList<Edge<I, E>>(Iterables.size(edges));
    for (Edge<I, E> edge : edges) {
      edgeList.add(EdgeFactory.create(
          WritableUtils.clone(edge.getTargetVertexId(), conf),
          WritableUtils.clone(edge.getValue(), conf)));
    }
    return edgeList;
  }

  /**
   * Compare two edge iterables up to reordering. The edge value type needs
   * to be Comparable.
   *
   * @param e1 First edge iterable
   * @param e2 Second edge iterable
   * @param <I> Vertex id
   * @param <E> Edge value
   * @return Whether the two iterables are equal up to reordering
   */
  public static <I extends WritableComparable, E extends WritableComparable>
  boolean sameEdges(Iterable<Edge<I, E>> e1, Iterable<Edge<I, E>> e2) {
    ArrayList<Edge<I, E>> edgeList1 = copy(e1);
    ArrayList<Edge<I, E>> edgeList2 = copy(e2);
    Comparator<Edge<I, E>> edgeComparator = new EdgeComparator<I, E>();
    Collections.sort(edgeList1, edgeComparator);
    Collections.sort(edgeList2, edgeComparator);
    return equals(edgeList1, edgeList2);
  }

  /**
   * Get the size of edges. Optimized implementation for cases when edges is
   * instance of {@link OutEdges} or {@link Collection} which only calls
   * size() method from these classes.
   *
   * @param edges Edges
   * @param <I> Vertex index
   * @param <E> Edge value
   * @return Size of edges
   */
  public static <I extends WritableComparable, E extends Writable> int size(
      Iterable<Edge<I, E>> edges) {
    if (edges instanceof OutEdges) {
      return ((OutEdges) edges).size();
    } else {
      return Iterables.size(edges);
    }
  }

  /**
   * Initialize edges data structure and add the edges from edgesIterable.
   *
   * If edgesIterable is instance of {@link OutEdges} or {@link Collection}
   * edges will be initialized with size of edgesIterable,
   * otherwise edges will be initialized without size.
   *
   * @param edges Edges to initialize
   * @param edgesIterable Iterable whose edges to use
   * @param <I> Vertex index
   * @param <E> Edge value
   */
  public static <I extends WritableComparable, E extends Writable>
  void initialize(OutEdges<I, E> edges, Iterable<Edge<I, E>> edgesIterable) {
    if (edgesIterable instanceof OutEdges ||
        edgesIterable instanceof Collection) {
      edges.initialize(size(edgesIterable));
    } else {
      edges.initialize();
    }
    for (Edge<I, E> edge : edgesIterable) {
      edges.add(edge);
    }
    if (edges instanceof Trimmable) {
      ((Trimmable) edges).trim();
    }
  }
}
