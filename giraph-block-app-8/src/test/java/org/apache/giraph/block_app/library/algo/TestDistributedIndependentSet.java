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
package org.apache.giraph.block_app.library.algo;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestDistributedIndependentSet {

  private void runTest(
      TestGraphModifier<LongWritable, DistributedIndependentSetVertexValue, NullWritable>
          graphLoader) throws Exception {
    TestGraphUtils.runTest(graphLoader, (graph) -> {
      checkDecomposition(graph);
    }, (conf) -> {
      BlockUtils.setBlockFactoryClass(conf, DistributedIndependentSetBlockFactory.class);
    });
  }

  private static void checkDecomposition(
      NumericTestGraph<LongWritable, DistributedIndependentSetVertexValue, NullWritable> graph) {
    final int UNASSIGNED = -1;
    final int HAS_EDGE_TO_IND_SET = -2;
    // Hold (id -> List of vertices) for each independent set id
    HashMap<Integer, ArrayList<Integer>> indSets = new HashMap<>();
    int numVertices = graph.getVertexCount();

    int numIndSets = 0;
    for (int i = 0; i < numVertices; ++i) {
      int indSetID = graph.getVertex(i).getValue().getIndependentSetID().get();
      // Number of independent sets are less than or equal to the number of vertices in the input
      // graph.
      Assert.assertTrue(indSetID >= 0 && indSetID < numVertices);
      // All tests assign (0, 1, ...) as independent set ids. The vertex assigned to the max id
      // is a specifier for the total number of independent sets the input graph is decomposed
      // into.
      if (indSetID > numIndSets)
        numIndSets = indSetID;
      ArrayList<Integer> mapValue = indSets.get(indSetID);
      if (mapValue == null) {
        mapValue = new ArrayList<>();
        indSets.put(indSetID, mapValue);
      }
      mapValue.add(i);
    }
    numIndSets++;

    int[] label = new int[numVertices];
    for (int i = 0; i < numVertices; ++i)
      label[i] = UNASSIGNED;

    for (int i = 0; i < numIndSets; ++i) {
      ArrayList<Integer> indSet = indSets.get(i);

      // All independent set ids are assigned consecutively starting from 0. There should be at
      // least one vertex per assigned independent set.
      Assert.assertTrue(indSet != null && indSet.size() > 0);
      for (int v : indSet) {
        // Check if vertices in this independent set is not assigned to any of the previous
        // independent sets.
        Assert.assertTrue(label[v] == UNASSIGNED);
        label[v] = i;
      }

      for (int v : indSet) {
        for (Edge<LongWritable, NullWritable> edge : graph.getVertex(v).getEdges()) {
          int u = (int) edge.getTargetVertexId().get();
          // Check all vertices in the current independent set do not have edge to each other.
          Assert.assertTrue(label[u] != label[v]);
          // Mark unassigned vertices neighboring this independent set. This is necessary to check
          // if this independent set is 'maximal'.
          if (label[u] == UNASSIGNED)
            label[u] = HAS_EDGE_TO_IND_SET;
        }
      }

      // Check if the independent set is maximal.
      for (int j = 0; j < numVertices; ++j) {
        Assert.assertTrue(label[j] != UNASSIGNED);
        // Reset marked vertices neighboring to this independent set.
        if (label[j] == HAS_EDGE_TO_IND_SET)
          label[j] = UNASSIGNED;
      }
    }
  }

  private static void createVertices(
      NumericTestGraph<LongWritable, DistributedIndependentSetVertexValue, NullWritable> graph,
      int numVertices) {
    for (int i = 0; i < numVertices; ++i)
      graph.addVertex(i);
  }

  @Test
  public void testSmallGraph() throws Exception {
    /*
     *   1      5
     *  / \    / \    6
     * 0---2--3---4
     */
    final int NUM_VERTICES = 7;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES);
      graph.addSymmetricEdge(0, 1);
      graph.addSymmetricEdge(0, 2);
      graph.addSymmetricEdge(1, 2);
      graph.addSymmetricEdge(2, 3);
      graph.addSymmetricEdge(3, 4);
      graph.addSymmetricEdge(3, 5);
      graph.addSymmetricEdge(4, 5);
    });
  }

  @Test
  public void testSmallGraphOrderingEffect() throws Exception {
    /*
     *   4      5
     *  / \    / \    6
     * 0---2--1---3
     */
    final int NUM_VERTICES = 7;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES);
      graph.addSymmetricEdge(0, 4);
      graph.addSymmetricEdge(0, 2);
      graph.addSymmetricEdge(2, 4);
      graph.addSymmetricEdge(1, 2);
      graph.addSymmetricEdge(1, 5);
      graph.addSymmetricEdge(1, 3);
      graph.addSymmetricEdge(3, 5);
    });
  }

  @Test
  public void testRingOdd() throws Exception {
    final int NUM_VERTICES = 13;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES);
      for (int i = 1; i < NUM_VERTICES; ++i)
        graph.addSymmetricEdge(i - 1, i);
      graph.addSymmetricEdge(0, NUM_VERTICES - 1);
    });
  }

  @Test
  public void testStarGraph() throws Exception {
    final int NUM_VERTICES = 15;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES);
      for (int i = 1; i < NUM_VERTICES; ++i)
        graph.addSymmetricEdge(0, i);
    });
  }

  @Test
  public void testMultipleStarGraphs() throws Exception {
    final int NUM_VERTICES1 = 15;
    final int NUM_VERTICES2 = 21;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES1 + NUM_VERTICES2);
      for (int i = 1; i < NUM_VERTICES1; ++i)
        graph.addSymmetricEdge(0, i);

      for (int i = 1 + NUM_VERTICES1; i < NUM_VERTICES1 + NUM_VERTICES2; ++i)
        graph.addSymmetricEdge(NUM_VERTICES1, i);
    });
  }

  @Test
  public void testMeshGraph() throws Exception {
    final int M = 11;
    final int N = 13;
    runTest((graph) -> {
      createVertices(graph, M * N);
      for (int i = 0; i < M; ++i)
        for (int j = 0; j < N; ++j) {
          if (i != M - 1)
            graph.addSymmetricEdge(i * N + j, (i + 1) * N + j);
          if (j != N - 1)
            graph.addSymmetricEdge(i * N + j, i * N + (j + 1));
        }
    });
  }

  @Test
  public void testCompleteGraph() throws Exception {
    final int NUM_VERTICES = 17;
    runTest((graph) -> {
      createVertices(graph, NUM_VERTICES);
      for (int i = 0; i < NUM_VERTICES; ++i)
        for (int j = i + 1; j < NUM_VERTICES; ++j)
          graph.addSymmetricEdge(i, j);
    });
  }
}
