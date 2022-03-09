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

import com.google.common.collect.ComparisonChain;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.WritableComparable;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for edges.
 *
 * @param <I> Vertex id
 * @param <E> Edge value (needs to be WritableComparable)
 */
public class EdgeComparator<I extends WritableComparable,
    E extends WritableComparable> implements Comparator<Edge<I, E>>,
    Serializable {
  /** Serialization version. */
  private static final long serialVersionUID = 1L;

  @Override
  public int compare(Edge<I, E> e1, Edge<I, E> e2) {
    return compareEdges(e1, e2);
  }

  /**
   * Compares two edges.
   *
   * @param e1 First edge
   * @param e2 Second edge
   * @param <I> Vertex id
   * @param <E> Edge value (needs to be WritableComparable)
   * @return A negative, zero, or positive value depending
   */
  public static <I extends WritableComparable, E extends WritableComparable>
  int compareEdges(Edge<I, E> e1, Edge<I, E> e2) {
    return ComparisonChain.start()
        .compare(e1.getTargetVertexId(), e2.getTargetVertexId())
        .compare(e1.getValue(), e2.getValue())
        .result();
  }

  /**
   * Indicates whether two edges are equal.
   *
   * @param e1 First edge
   * @param e2 Second edge
   * @param <I> Vertex id
   * @param <E> Edge value (needs to be WritableComparable)
   * @return Whether the two edges are equal
   */
  public static <I extends WritableComparable, E extends WritableComparable>
  boolean equal(Edge<I, E> e1, Edge<I, E> e2) {
    return compareEdges(e1, e2) == 0;
  }
}
