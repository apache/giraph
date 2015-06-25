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
 * Create a network that looks like:
 *   1      5
 *  / \    / \    6
 * 0---2  3---4
 *
 * where 6 is disconnected from the rest of the network.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class Small2GraphInit<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements TestGraphModifier<I, V, E> {
  private final Supplier<E> edgeSupplier;

  public Small2GraphInit() {
    this(null);
  }

  public Small2GraphInit(Supplier<E> edgeSupplier) {
    this.edgeSupplier = edgeSupplier;
  }

  @Override
  public void modifyGraph(NumericTestGraph<I, V, E> graph) {
    graph.addSymmetricEdge(0, 1, createEdgeValue());
    graph.addSymmetricEdge(0, 2, createEdgeValue());
    graph.addSymmetricEdge(1, 2, createEdgeValue());
    graph.addSymmetricEdge(3, 4, createEdgeValue());
    graph.addSymmetricEdge(3, 5, createEdgeValue());
    graph.addSymmetricEdge(4, 5, createEdgeValue());

    graph.addVertex(6);
  }

  private E createEdgeValue() {
    return edgeSupplier != null ? edgeSupplier.get() : null;
  }
}

