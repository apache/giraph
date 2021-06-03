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
package org.apache.giraph.block_app.examples.pagerank;

import org.apache.giraph.block_app.framework.BlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.EachVertexInit;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;

public class TestPageRankExample {
  private <I extends WritableComparable>
  void testTenIterations(final Class<I> type,
      final Class<? extends BlockFactory<?>> factory) throws Exception {
    TestGraphUtils.runTest(
        TestGraphUtils.chainModifiers(
            new Small1GraphInit<LongWritable, DoubleWritable, NullWritable>(),
            new EachVertexInit<>((vertex) -> vertex.getValue().set(1.0))),
        (graph) -> {
          float outside = 0.8759f;
          float inside = 1.2481f;
          float isolated = 0.15f;

          for (int i : new int[] {0, 1, 4, 5}) {
            Assert.assertEquals(outside, graph.getValue(i).get(), 0.001);
          }
          for (int i : new int[] {2, 3}) {
            Assert.assertEquals(inside, graph.getValue(i).get(), 0.001);
          }
          for (int i : new int[] {6}) {
            Assert.assertEquals(isolated, graph.getValue(i).get(), 0.001);
          }
        },
        (GiraphConfiguration conf) -> {
          GiraphConstants.VERTEX_ID_CLASS.set(conf, type);
          BlockUtils.setBlockFactoryClass(conf, factory);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 10);
        });
  }

  private void testTenIterationsOnAllTypes(Class<? extends BlockFactory<?>> factory)
      throws Exception {
    testTenIterations(
        IntWritable.class,
        factory);
    testTenIterations(
        LongWritable.class,
        factory);
  }

  private <I extends WritableComparable>
  void testConverging(
      final Class<I> type, Class<? extends BlockFactory<?>> factory) throws Exception {
    TestGraphUtils.runTest(
        new Small1GraphInit<LongWritable, DoubleWritable, NullWritable>(),
        (graph) -> {
        },
        (GiraphConfiguration conf) -> {
          GiraphConstants.VERTEX_ID_CLASS.set(conf, type);
          BlockUtils.setBlockFactoryClass(conf, factory);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, Integer.MAX_VALUE);
        });
    // Test is that this doesn't loop forever, even though num_iterations=Integer.MAX_VALUE
  }

  private <I extends WritableComparable>
  void testConvergingOnAllTypes(Class<? extends BlockFactory<?>> factory) throws Exception {
    testConverging(
        IntWritable.class,
        factory);
    testConverging(
        LongWritable.class,
        factory);
  }

  @Test
  public void testExample() throws Exception {
    testTenIterationsOnAllTypes(PageRankWithPiecesExampleBlockFactory.class);
  }

  @Test
  public void testConvergenceExample() throws Exception {
    testTenIterationsOnAllTypes(PageRankWithPiecesAndConvergenceExampleBlockFactory.class);
    testConvergingOnAllTypes(PageRankWithPiecesAndConvergenceExampleBlockFactory.class);
  }

  @Test
  public void testFunctionalExample() throws Exception {
    testTenIterationsOnAllTypes(PageRankExampleBlockFactory.class);
  }

  @Test
  public void testFunctionalConvergenceExample() throws Exception {
    testTenIterationsOnAllTypes(PageRankWithTransferAndConvergenceExampleBlockFactory.class);
    testConvergingOnAllTypes(PageRankWithTransferAndConvergenceExampleBlockFactory.class);
  }

  @Test
  public void testFunctionalChainConvergenceExample() throws Exception {
    testTenIterationsOnAllTypes(PageRankWithConvergenceExampleBlockFactory.class);
    testConvergingOnAllTypes(PageRankWithConvergenceExampleBlockFactory.class);
  }
}
