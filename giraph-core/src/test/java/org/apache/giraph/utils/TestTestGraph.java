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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Test TestGraph
 */
public class TestTestGraph {
  @Test
  public void testTestGraph() {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, NullWritable.class);
    conf.setVertexValueCombinerClass(SumLongVertexValueCombiner.class);
    conf.setOutEdgesClass(HashMapEdges.class);
    TestGraph<LongWritable, LongWritable, NullWritable> testGraph =
        new TestGraph<>(conf);
    addVertex(testGraph, 1, 10, 2, 3);
    addVertex(testGraph, 2, 20, 1, 3);
    addVertex(testGraph, 3, 30, 1, 2);
    addVertex(testGraph, 1, 100, 3, 4);
    addVertex(testGraph, 2, 200, 5, 1, 6);

    Vertex<LongWritable, LongWritable, NullWritable> vertex1 =
        testGraph.getVertex(new LongWritable(1));
    Assert.assertEquals(110, vertex1.getValue().get());
    Assert.assertEquals(3, vertex1.getNumEdges());

    Vertex<LongWritable, LongWritable, NullWritable> vertex2 =
        testGraph.getVertex(new LongWritable(2));
    Assert.assertEquals(220, vertex2.getValue().get());
    Assert.assertEquals(4, vertex2.getNumEdges());

    Vertex<LongWritable, LongWritable, NullWritable> vertex3 =
        testGraph.getVertex(new LongWritable(3));
    Assert.assertEquals(30, vertex3.getValue().get());
    Assert.assertEquals(2, vertex3.getNumEdges());
  }

  public static void addVertex(
      TestGraph<LongWritable, LongWritable, NullWritable> graph, long id,
      long value, long... neighbors) {
    Map.Entry<LongWritable, NullWritable> edges[] =
        new Map.Entry[neighbors.length];
    for (int i = 0; i < neighbors.length; i++) {
      edges[i] = new AbstractMap.SimpleEntry<>(
          new LongWritable(neighbors[i]), NullWritable.get());
    }
    graph.addVertex(new LongWritable(id), new LongWritable(value), edges);
  }

  /**
   * Vertex value combiner that sums up long vertex values
   */
  public static class SumLongVertexValueCombiner
      implements VertexValueCombiner<LongWritable> {
    @Override
    public void combine(LongWritable originalVertexValue,
        LongWritable vertexValue) {
      originalVertexValue.set(originalVertexValue.get() + vertexValue.get());
    }
  }
}
