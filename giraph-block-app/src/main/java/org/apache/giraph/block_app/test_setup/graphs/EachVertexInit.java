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
import org.apache.giraph.function.Consumer;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Traverse each Vertex in the graph, and initialize it with a given
 * consumer function.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class EachVertexInit<I extends WritableComparable, V extends Writable,
    E extends Writable> implements TestGraphModifier<I, V, E> {
  private final Consumer<Vertex<I, V, E>> vertexConsumer;

  public EachVertexInit(Consumer<Vertex<I, V, E>> vertexConsumer) {
    this.vertexConsumer = vertexConsumer;
  }

  @Override
  public void modifyGraph(NumericTestGraph<I, V, E> graph) {
    for (Vertex<I, V, E> vertex : graph.getTestGraph()) {
      vertexConsumer.apply(vertex);
    }
  }
}
