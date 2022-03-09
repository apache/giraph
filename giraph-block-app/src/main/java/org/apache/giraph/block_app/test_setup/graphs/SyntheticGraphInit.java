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

import java.util.Random;

import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Creates synthetic graphs, that can have community structure.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class SyntheticGraphInit<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements TestGraphModifier<I, V, E> {
  public static final IntConfOption NUM_COMMUNITIES = new IntConfOption(
      "test.SyntheticGraphCreator.NUM_COMMUNITIES", -1, "");
  public static final IntConfOption NUM_VERTICES = new IntConfOption(
      "test.SyntheticGraphCreator.NUM_VERTICES", -1, "");
  public static final IntConfOption NUM_EDGES_PER_VERTEX = new IntConfOption(
      "test.SyntheticGraphCreator.NUM_EDGES_PER_VERTEX", -1, "");
  public static final FloatConfOption ACTUAL_LOCALITY_RATIO =
      new FloatConfOption(
            "test.SyntheticGraphCreator.ACTUAL_LOCALITY_RATIO", -1, "");

  protected final Supplier<E> edgeSupplier;

  public SyntheticGraphInit(Supplier<E> edgeSupplier) {
    this.edgeSupplier = edgeSupplier;
  }

  public SyntheticGraphInit() {
    this.edgeSupplier = null;
  }

  @Override
  public void modifyGraph(NumericTestGraph<I, V, E> graph) {
    GiraphConfiguration conf = graph.getConf();
    int numPartitions = NUM_COMMUNITIES.get(conf);
    int numVertices = NUM_VERTICES.get(conf);
    int numEdgesPerVertex = NUM_EDGES_PER_VERTEX.get(conf);
    int communitySize = numVertices / numPartitions;
    float actualLocalityRatio = ACTUAL_LOCALITY_RATIO.get(conf);
    Random random = new Random(42);
    for (int i = 0; i < numVertices; ++i) {
      for (int e = 0; e < numEdgesPerVertex / 2; ++e) {
        boolean localEdge = random.nextFloat() < actualLocalityRatio;
        int community = i / communitySize;
        int j;
        do {
          if (localEdge) {
            j = community * communitySize + random.nextInt(communitySize);
          } else {
            j = random.nextInt(numVertices);
          }
        } while (j == i);
        graph.addSymmetricEdge(
            i, j, edgeSupplier != null ? edgeSupplier.get() : null);
      }
    }
  }
}
