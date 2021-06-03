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
package org.apache.giraph.block_app.library.coarsening;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.library.ReusableSuppliers;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.EachVertexInit;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestCoarseningUtils {

  public static class TestCoarseningUtilsBlockFactory extends AbstractBlockFactory<Object> {
    @Override
    public Block createBlock(GiraphConfiguration conf) {
      return CoarseningUtils.<IntWritable, IntWritable, IntWritable>createCoarseningBlock(
          (ImmutableClassesGiraphConfiguration<IntWritable, IntWritable, IntWritable>) conf,
          ReusableSuppliers.fromInt((vertex) -> {
            int id = vertex.getId().get();
            if (id == 0 || id == 1) {
              return -1;
            } else if (id == 2 || id == 3) {
              return -2;
            } else if (id == 4 || id == 5) {
              return -4;
            } else return -id;
          }),
          id -> id.get() >= 0,
          id -> id.get() < 0);
    }

    @Override
    public Object createExecutionStage(GiraphConfiguration conf) {
      return new Object();
    }

    @Override
    protected Class<IntWritable> getVertexIDClass(GiraphConfiguration conf) {
      return IntWritable.class;
    }

    @Override
    protected Class<IntWritable> getVertexValueClass(GiraphConfiguration conf) {
      return IntWritable.class;
    }

    @Override
    protected Class<IntWritable> getEdgeValueClass(GiraphConfiguration conf) {
      return IntWritable.class;
    }
  }

  @Test
  public void testSmallGraph() throws Exception {
    /* We take small graph:
     *
     *   1      5
     *  / \    / \    6
     * 0---2--3---4
     *
     * And coarsen it into 4 groups: (0,1), (2,3), (4,5), (6)
     */

    TestGraphUtils.<IntWritable, IntWritable, IntWritable>runTest(
        TestGraphUtils.chainModifiers(
            new Small1GraphInit<>(() -> new IntWritable(1)),
            new EachVertexInit<>((vertex) -> vertex.getValue().set(1))),
        (graph) -> {
          for (int i : new int[] {-1, -4}) {
            Assert.assertEquals(2, graph.getValue(i).get());
            Assert.assertEquals(2, graph.getVertex(i).getNumEdges());

            Map<Integer, Integer> edges = edgesToMap(graph.getVertex(i).getEdges());
            // self loop
            Assert.assertEquals(2, edges.get(i).intValue());

            Assert.assertEquals(2, edges.get(-2).intValue());
          }
          for (int i : new int[] {-2}) {
            Assert.assertEquals(2, graph.getValue(i).get());
            Assert.assertEquals(3, graph.getVertex(i).getNumEdges());

            Map<Integer, Integer> edges = edgesToMap(graph.getVertex(i).getEdges());
            // self loop
            Assert.assertEquals(2, edges.get(i).intValue());

            Assert.assertEquals(2, edges.get(-1).intValue());
            Assert.assertEquals(2, edges.get(-4).intValue());
          }
          for (int i : new int[] {-6}) {
            Assert.assertEquals(1, graph.getValue(i).get());
            Assert.assertEquals(0, graph.getVertex(i).getNumEdges());
          }
        },
        (conf) -> {
          BlockUtils.setBlockFactoryClass(conf, TestCoarseningUtilsBlockFactory.class);
        });
  }

  private static Map<Integer, Integer> edgesToMap(Iterable<Edge<IntWritable, IntWritable>> edges) {
    Map<Integer, Integer> map = new HashMap<>();
    for(Edge<IntWritable, IntWritable> edge : edges) {
      Assert.assertNull(map.put(edge.getTargetVertexId().get(), edge.getValue().get()));
    }
    return map;
  }
}
