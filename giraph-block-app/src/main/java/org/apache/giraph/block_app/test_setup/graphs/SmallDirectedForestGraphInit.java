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
package org.apache.giraph.block_app.test_setup.graphs;

import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Create a directed forest that looks like:
 *
 *   0      4     6
 *  / \     |    / \
 * 1   2    5   7   8
 *     |
 *     3
 *
 * Edges are directed from top to bottom.
 * Vertices with no edges are created.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class SmallDirectedForestGraphInit<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements TestGraphModifier<I, V, E> {

  private final Supplier<E> edgeSupplier;

  public SmallDirectedForestGraphInit() {
    this(null);
  }

  public SmallDirectedForestGraphInit(Supplier<E> edgeSupplier) {
    this.edgeSupplier = edgeSupplier;
  }

  @Override
  public void modifyGraph(NumericTestGraph<I, V, E> graph) {
    graph.addEdge(0, 1, createEdgeValue());
    graph.addEdge(0, 2, createEdgeValue());
    graph.addEdge(2, 3, createEdgeValue());
    graph.addEdge(4, 5, createEdgeValue());
    graph.addEdge(6, 7, createEdgeValue());
    graph.addEdge(6, 8, createEdgeValue());

    graph.addVertex(1);
    graph.addVertex(3);
    graph.addVertex(5);
    graph.addVertex(7);
    graph.addVertex(8);
  }

  private E createEdgeValue() {
    return edgeSupplier != null ? edgeSupplier.get() : null;
  }
}
