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

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Utilities for converting between edge iterables and neighbor iterables.
 */
public class EdgeIterables {
  /** Utility classes shouldn't be instantiated. */
  private EdgeIterables() { }

  /**
   * Convert an edge iterable into a neighbor iterable.
   *
   * @param edges Edge iterable.
   * @param <I> Vertex id type.
   * @param <E> Edge value type.
   * @return Neighbor iterable.
   */
  public static
  <I extends WritableComparable, E extends Writable>
  Iterable<I> getNeighbors(Iterable<Edge<I, E>> edges) {
    return Iterables.transform(edges,
        new Function<Edge<I, E>, I>() {
          @Override
          public I apply(Edge<I, E> edge) {
            return edge == null ? null : edge.getTargetVertexId();
          }
        });
  }

  /**
   * Convert a neighbor iterable into an edge iterable.
   *
   * @param neighbors Neighbor iterable.
   * @param <I> Vertex id type.
   * @return Edge iterable.
   */
  public static <I extends WritableComparable>
  Iterable<Edge<I, NullWritable>> getEdges(Iterable<I> neighbors) {
    return Iterables.transform(neighbors,
        new Function<I, Edge<I, NullWritable>>() {
          @Override
          public Edge<I, NullWritable> apply(I neighbor) {
            return EdgeFactory.create(neighbor);
          }
        });
  }
}
