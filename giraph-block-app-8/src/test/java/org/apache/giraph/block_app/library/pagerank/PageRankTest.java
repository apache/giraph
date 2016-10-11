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
package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pagerank test
 */
public class PageRankTest {
  private static final int NUMBER_OF_ITERATIONS = 50;
  private static final double PRECISION = 0.0000001;

  public static void testComputation(ExampleGenerator generator)
      throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    PageRankSettings.ITERATIONS.set(conf, NUMBER_OF_ITERATIONS);
    BlockUtils.setAndInitBlockFactoryClass(conf, PageRankBlockFactory.class);

    final WeightedPageRankTestExample example = generator.generate(conf);

    LocalBlockRunner.runAppWithVertexOutput(example.graph, (vertex) -> {
      Long id = vertex.getId().get();
      double expected = example.expectedOutput.get(id);
      double received = vertex.getValue().get();
      Assert.assertEquals(expected, received, PRECISION);
    });
  }

  @Test
  public void testCliqueWeightedVertex() throws Exception {
    testComputation(PageRankTest::createCliqueExample);
  }

  @Test
  public void testRingWeightedVertex() throws Exception {
    testComputation(PageRankTest::createRingExample);
  }

  @Test
  public void testOneVertexConnectedToAllWeightedVertex() throws Exception {
    testComputation(PageRankTest::createOneVertexConnectedToAllExample);
  }

  @Test
  public void testAllVerticesConnectedToOneWeightedVertex() throws Exception {
    testComputation(PageRankTest::createAllVerticesConnectedToOne);
  }

  @Test
  public void testSmallChainWeightedVertex() throws Exception {
    testComputation(PageRankTest::createSmallChainExample);
  }

  @Test
  public void compareWithUnweightedPageRank() throws Exception {
    int numVertices = 100;
    int maxEdges = 50;
    float dampingFactor = 0.85f;

    GiraphConfiguration wprConf = new GiraphConfiguration();
    PageRankSettings.WEIGHTED_PAGERANK.set(wprConf, true);
    PageRankSettings.ITERATIONS.set(wprConf, NUMBER_OF_ITERATIONS);
    PageRankSettings.DAMPING_FACTOR.set(wprConf, dampingFactor);
    BlockUtils.setAndInitBlockFactoryClass(wprConf, PageRankBlockFactory.class);

    GiraphConfiguration prConf = new GiraphConfiguration();
    PageRankSettings.WEIGHTED_PAGERANK.set(prConf, false);
    PageRankSettings.ITERATIONS.set(prConf, NUMBER_OF_ITERATIONS);
    PageRankSettings.DAMPING_FACTOR.set(prConf, dampingFactor);
    BlockUtils.setAndInitBlockFactoryClass(prConf, PageRankBlockFactory.class);

    TestGraph<LongWritable, DoubleWritable, DoubleWritable> wprGraph =
        new TestGraph<>(wprConf);
    TestGraph<LongWritable, DoubleWritable, NullWritable> prGraph =
        new TestGraph<>(prConf);
    for (int i = 0; i < numVertices; i++) {
      int[] neighbors = new int[(int) (Math.random() * maxEdges)];
      double[] edgeWeights = new double[neighbors.length];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = (int) (Math.random() * numVertices);
        edgeWeights[j] = 1.0;
      }
      prGraph.addVertex(new LongWritable(i), new DoubleWritable(1.0),
          createEdgesWeightless(neighbors));
      wprGraph.addVertex(new LongWritable(i), new DoubleWritable(1.0),
          createEdges(neighbors, edgeWeights));
    }

    wprGraph = InternalVertexRunner.runWithInMemoryOutput(wprConf, wprGraph);

    prGraph = InternalVertexRunner.runWithInMemoryOutput(prConf, prGraph);

    for (Vertex<LongWritable, DoubleWritable, DoubleWritable> wprVertex : wprGraph) {
      Vertex<LongWritable, DoubleWritable, NullWritable> prVertex =
          prGraph.getVertex(wprVertex.getId());
      Assert.assertEquals(prVertex.getValue().get(), wprVertex.getValue().get(), PRECISION);
    }
  }

  /**
   * Creates a map of weighted edges from neighbors and edgeWeights
   *
   * @param neighbors  neighbors
   * @param edgeWeights edgeWeights
   * @return  returns the edges
   */
  private static Map.Entry<LongWritable, DoubleWritable>[] createEdges(int[] neighbors,
                                                                       double[] edgeWeights) {
    Map.Entry<LongWritable, DoubleWritable>[] edges = new Map.Entry[neighbors.length];
    for (int i = 0; i < neighbors.length; i++) {
      edges[i] = new AbstractMap.SimpleEntry<>(
          new LongWritable(neighbors[i]), new DoubleWritable(edgeWeights[i]));
    }
    return edges;
  }

  /**
   * Creates a map of unweighted edges from neighbors
   *
   * @param neighbors  neighbors
   * @return  returns the edges
   */
  private static Map.Entry<LongWritable, NullWritable>[] createEdgesWeightless(int[] neighbors) {
    Map.Entry<LongWritable, NullWritable>[] edges = new Map.Entry[neighbors.length];
    for (int i = 0; i < neighbors.length; i++) {
      edges[i] = new AbstractMap.SimpleEntry<>(
          new LongWritable(neighbors[i]), NullWritable.get());
    }
    return edges;
  }



  /**
   * Helper class for data related to one test case for weighted page rank.
   */
  private static class WeightedPageRankTestExample {
    TestGraph<LongWritable, DoubleWritable, DoubleWritable> graph;
    Map<Long, Double> expectedOutput = new HashMap<>();
  }

  private interface ExampleGenerator {
    WeightedPageRankTestExample generate(GiraphConfiguration conf);
  }

  /**
   * Create test case when graph is a clique. All outgoing edges from one
   * vertex have the same weights, so they will be normalized to 1/n,
   * and all vertices should have page rank 1.
   */
  private static WeightedPageRankTestExample createCliqueExample(GiraphConfiguration conf) {
    WeightedPageRankTestExample example = new WeightedPageRankTestExample();
    example.graph = new TestGraph<>(conf);
    addVertex(1, new long[] {2, 3, 4}, new double[] {1, 1, 1}, example.graph);
    addVertex(2, new long[] {1, 3, 4}, new double[] {2, 2, 2}, example.graph);
    addVertex(3, new long[] {1, 2, 4}, new double[] {0.1, 0.1, 0.1}, example.graph);
    addVertex(4, new long[] {1, 2, 3}, new double[] {5, 5, 5}, example.graph);
    example.expectedOutput.put(1L, 1.0);
    example.expectedOutput.put(2L, 1.0);
    example.expectedOutput.put(3L, 1.0);
    example.expectedOutput.put(4L, 1.0);
    return example;
  }


  public static void addVertex(int id, long[] edges, double[] weights,
      TestGraph<LongWritable, DoubleWritable, DoubleWritable> graph) {
    Vertex<LongWritable, DoubleWritable, DoubleWritable> v = graph.getConf().createVertex();
    v.setConf(graph.getConf());
    v.initialize(new LongWritable(id), new DoubleWritable(), newEdges(edges, weights));
    graph.addVertex(v);
  }

  private static Iterable<Edge<LongWritable, DoubleWritable>> newEdges(long[] ids, double[] weights) {
    List<Edge<LongWritable, DoubleWritable>> edges = Lists.newArrayListWithCapacity(ids.length);
    for (int i = 0; i < ids.length; i++) {
      edges.add(EdgeFactory.create(new LongWritable(ids[i]), new DoubleWritable(weights[i])));
    }
    return edges;
  }

  /**
   * Create test case when graph is a simple cycle. All vertices have just
   * one outgoing edge, so their weights are all going to be normalized to 1,
   * and all vertices should have page rank 1.
   */
  private static WeightedPageRankTestExample createRingExample(GiraphConfiguration conf) {
    WeightedPageRankTestExample example = new WeightedPageRankTestExample();
    example.graph = new TestGraph<>(conf);
    addVertex(1, new long[] {2}, new double[] {1}, example.graph);
    addVertex(2, new long[] {3}, new double[] {2}, example.graph);
    addVertex(3, new long[] {4}, new double[] {1}, example.graph);
    addVertex(4, new long[] {5}, new double[] {5}, example.graph);
    addVertex(5, new long[] {6}, new double[] {0.7}, example.graph);
    addVertex(6, new long[] {7}, new double[] {2}, example.graph);
    addVertex(7, new long[] {8}, new double[] {0.3}, example.graph);
    addVertex(8, new long[] {1}, new double[] {5}, example.graph);

    for (long i = 1; i <= 8; i++) {
      example.expectedOutput.put(i, 1.0);
    }
    return example;
  }

  /**
   * Create test case when we have one vertex X which has outgoing edges to
   * all other vertices, and all other vertices have just one outgoing
   * edge to X.
   * Page rank of X should be (1 + d * (n - 1)) / (d + 1),
   * where d is dumping factor and n total number of vertices.
   * Page rank of some other vertex Y should be 1 - d + d * pr(X) * y,
   * where y is normalized weight of edge X->Y.
   */
  private static WeightedPageRankTestExample
  createOneVertexConnectedToAllExample(GiraphConfiguration conf) {
    WeightedPageRankTestExample example = new WeightedPageRankTestExample();
    PageRankSettings.DAMPING_FACTOR.set(conf, 0.85f);
    example.graph = new TestGraph<>(conf);
    addVertex(1, new long[] {2, 3, 4, 5}, new double[] {1, 2, 3, 4}, example.graph);
    addVertex(2, new long[] {1}, new double[] {2}, example.graph);
    addVertex(3, new long[] {1}, new double[] {0.1}, example.graph);
    addVertex(4, new long[] {1}, new double[] {5}, example.graph);
    addVertex(5, new long[] {1}, new double[] {5}, example.graph);
    // these values are obtained from eigenvector calculation in numpy
    example.expectedOutput.put(1L, 2.37797072308);
    example.expectedOutput.put(2L, 0.35220291338);
    example.expectedOutput.put(3L, 0.55440585061);
    example.expectedOutput.put(4L, 0.75660878784);
    example.expectedOutput.put(5L, 0.95881172507);
    return example;
  }

  /**
   * Create test case when we have 4 vertices, A,B,C,D,
   * with edges in both directions between A and B, B and C, and C and D.
   * If d is dumping factor, b weight of edge B->A and c weight of edge
   * C->D, the formulas below are calculating the page rank of vertices.
   */
  private static WeightedPageRankTestExample createSmallChainExample(GiraphConfiguration conf) {
    WeightedPageRankTestExample example = new WeightedPageRankTestExample();
    PageRankSettings.DAMPING_FACTOR.set(conf, 0.9f);
    example.graph = new TestGraph<>(conf);
    addVertex(1, new long[] {2}, new double[] {3}, example.graph);
    addVertex(2, new long[] {1, 3}, new double[] {3, 7}, example.graph);
    addVertex(3, new long[] {2, 4}, new double[] {4, 6}, example.graph);
    addVertex(4, new long[] {3}, new double[] {5}, example.graph);
    // these values are obtained from eigenvector calculation in numpy
    example.expectedOutput.put(1L, 0.3762585);
    example.expectedOutput.put(2L, 1.0231795);
    example.expectedOutput.put(3L, 1.62374149);
    example.expectedOutput.put(4L, 0.97682040);
    return example;
  }

  /**
   * Create a test with 4 vertices, 3 vertices are connected to the first
   * Tests the code in presence of sinks / dangling vertices
   * @return example
   */
  private static WeightedPageRankTestExample createAllVerticesConnectedToOne(GiraphConfiguration conf) {
    WeightedPageRankTestExample example = new WeightedPageRankTestExample();
    PageRankSettings.DAMPING_FACTOR.set(conf, 0.85f);
    example.graph = new TestGraph<>(conf);
    addVertex(1, new long[] {}, new double[] {}, example.graph);
    addVertex(2, new long[] {1}, new double[] {3}, example.graph);
    addVertex(3, new long[] {1}, new double[] {4}, example.graph);
    addVertex(4, new long[] {1}, new double[] {5}, example.graph);
    // these values are obtained from eigenvector calculation in numpy
    example.expectedOutput.put(1L, 2.16793893);
    example.expectedOutput.put(2L, 0.61068702);
    example.expectedOutput.put(3L, 0.61068702);
    example.expectedOutput.put(4L, 0.61068702);
    return example;
  }
}
