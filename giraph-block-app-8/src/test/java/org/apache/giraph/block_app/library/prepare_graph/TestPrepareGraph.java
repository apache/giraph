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
package org.apache.giraph.block_app.library.prepare_graph;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.prepare_graph.TestPrepareGraph.TmpBlockFactory.TmpRunBlockFactory;
import org.apache.giraph.block_app.library.prepare_graph.TestPrepareGraph.TmpBlockFactory.TmpRunBlockFactory2;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class TestPrepareGraph {
  public static @DataPoints boolean[] booleanOptions = {false, true};

  @Theory
  public void test1(boolean fullGiraphEnv) throws Exception {
    TestGraphUtils.runTest(
        new CreateTestGraph(),
        (graph) -> {
          Assert.assertTrue(connected(graph, 0, 1));
          Assert.assertTrue(connected(graph, 1, 0));
          Assert.assertTrue(!connected(graph, 1, 2));
          Assert.assertNull(graph.getVertex(2));
          Assert.assertNull(graph.getVertex(3));
        },
        (conf) -> {
          BlockUtils.setBlockFactoryClass(conf, TmpRunBlockFactory.class);
          TestGraphUtils.USE_FULL_GIRAPH_ENV_IN_TESTS.set(conf, fullGiraphEnv);
        });
  }

  @Theory
  public void test2(boolean fullGiraphEnv) throws Exception {
    TestGraphUtils.runTest(
        new CreateTestGraph(),
        (graph) -> {
          Assert.assertTrue(connected(graph, 0, 1));
          Assert.assertTrue(connected(graph, 1, 0));
          Assert.assertTrue(connected(graph, 1, 2));
          Assert.assertTrue(connected(graph, 2, 1));
          Assert.assertNull(graph.getVertex(3));
        },
        (conf) -> {
          BlockUtils.setBlockFactoryClass(conf, TmpRunBlockFactory2.class);
          TestGraphUtils.USE_FULL_GIRAPH_ENV_IN_TESTS.set(conf, fullGiraphEnv);
        });
  }

  private static boolean connected(
      NumericTestGraph<LongWritable, NullWritable, NullWritable> graph, int from,
      int to) {
    for (Edge<LongWritable, NullWritable> edge : graph.getVertex(from).getEdges()) {
      if (edge.getTargetVertexId().get() == to) {
        return true;
      }
    }
    return false;
  }

  private static class CreateTestGraph
      implements TestGraphModifier<LongWritable, NullWritable, NullWritable> {
    @Override
    public void modifyGraph(NumericTestGraph<LongWritable, NullWritable, NullWritable> graph) {
      graph.addEdge(0, 1);
      graph.addEdge(1, 0);
      graph.addEdge(1, 2);
    }
  }


  /**
   * class used for testing for building the block to test
   */
  public static abstract class TmpBlockFactory extends AbstractBlockFactory<Object> {
    @Override
    public Object createExecutionStage(GiraphConfiguration conf) {
      return new Object();
    }

    @Override
    protected Class<LongWritable> getVertexIDClass(GiraphConfiguration conf) {
      return LongWritable.class;
    }

    @Override
    protected Class<NullWritable> getVertexValueClass(GiraphConfiguration conf) {
      return NullWritable.class;
    }

    @Override
    protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration conf) {
      return NullWritable.class;
    }

    /**
     * Temporary factory that creates a sequence of pieces to remove asymmetric edges
     * and standing alone vertices.(Used for testing)
     */
    public static class TmpRunBlockFactory extends TmpBlockFactory {
      @Override
      public Block createBlock(GiraphConfiguration conf) {
        return new SequenceBlock(PrepareGraphPieces.removeAsymEdges(LongTypeOps.INSTANCE),
            PrepareGraphPieces.removeStandAloneVertices());
      }
    }

    /**
     * Temporary factory that creates a sequence of pieces to make the graph symmetric
     * and remove standing alone vertices.(Used for testing)
     */
    public static class TmpRunBlockFactory2 extends TmpBlockFactory {
      // relies on shouldCreateVertexOnMsgs=true

      @Override
      public Block createBlock(GiraphConfiguration conf) {
        return new SequenceBlock(PrepareGraphPieces.makeSymmetricUnweighted(LongTypeOps.INSTANCE),
            PrepareGraphPieces.removeStandAloneVertices());
      }
    }
  }
}
