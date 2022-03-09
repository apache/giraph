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
package org.apache.giraph.block_app.framework.api.local;

import org.apache.giraph.block_app.examples.pagerank.AbstractPageRankExampleBlockFactory;
import org.apache.giraph.block_app.examples.pagerank.PageRankExampleBlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.EachVertexInit;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.block_app.test_setup.graphs.SyntheticGraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestLocalBlockRunnerSpeed {
  public static class EmptyPiecesBlockFactory extends AbstractPageRankExampleBlockFactory {
    @Override
    public Block createBlock(GiraphConfiguration conf) {
      return new RepeatBlock(NUM_ITERATIONS.get(conf), new Piece<>());
    }
  }

  @BeforeClass
  public static void warmup() throws Exception {
    TestGraphUtils.runTest(
        new Small1GraphInit<LongWritable, DoubleWritable, NullWritable>(),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, EmptyPiecesBlockFactory.class);
          BlockUtils.LOG_EXECUTION_STATUS.set(conf, false);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 1000);
        });
  }

  @Test
  @Ignore("use for benchmarking")
  public void testEmptyIterationsSmallGraph() throws Exception {
    TestGraphUtils.runTest(
        new Small1GraphInit<LongWritable, DoubleWritable, NullWritable>(),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, EmptyPiecesBlockFactory.class);
          BlockUtils.LOG_EXECUTION_STATUS.set(conf, false);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 10000);
        });
  }

  @Test
  @Ignore("use for benchmarking")
  public void testEmptyIterationsSyntheticGraphLowDegree() throws Exception {
    TestGraphUtils.runTest(
        new SyntheticGraphInit<LongWritable, DoubleWritable, NullWritable>(),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, EmptyPiecesBlockFactory.class);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 1000);

          SyntheticGraphInit.NUM_VERTICES.set(conf, 500000);
          SyntheticGraphInit.NUM_EDGES_PER_VERTEX.set(conf, 10);
          SyntheticGraphInit.NUM_COMMUNITIES.set(conf, 1000);
        });
  }

  @Test
  @Ignore("use for benchmarking")
  public void testEmptyIterationsSyntheticGraphHighDegree() throws Exception {
    TestGraphUtils.runTest(
        new SyntheticGraphInit<LongWritable, DoubleWritable, NullWritable>(),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, EmptyPiecesBlockFactory.class);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 1000);

          SyntheticGraphInit.NUM_VERTICES.set(conf, 50000);
          SyntheticGraphInit.NUM_EDGES_PER_VERTEX.set(conf, 100);
          SyntheticGraphInit.NUM_COMMUNITIES.set(conf, 1000);
        });
  }

  @Test
  @Ignore("use for benchmarking")
  public void testPageRankSyntheticGraphLowDegree() throws Exception {
    TestGraphUtils.runTest(
        TestGraphUtils.chainModifiers(
            new SyntheticGraphInit<LongWritable, DoubleWritable, NullWritable>(),
            new EachVertexInit<>((vertex) -> vertex.getValue().set(1.0))),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, PageRankExampleBlockFactory.class);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 100);

          SyntheticGraphInit.NUM_VERTICES.set(conf, 500000);
          SyntheticGraphInit.NUM_EDGES_PER_VERTEX.set(conf, 10);
          SyntheticGraphInit.NUM_COMMUNITIES.set(conf, 1000);
        });
  }

  @Test
  @Ignore("use for benchmarking")
  public void testPageRankSyntheticGraphHighDegree() throws Exception {
    TestGraphUtils.runTest(
        TestGraphUtils.chainModifiers(
            new SyntheticGraphInit<LongWritable, DoubleWritable, NullWritable>(),
            new EachVertexInit<>((vertex) -> vertex.getValue().set(1.0))),
        null,
        (GiraphConfiguration conf) -> {
          LocalBlockRunner.RUN_ALL_CHECKS.set(conf, false);
          BlockUtils.setBlockFactoryClass(conf, PageRankExampleBlockFactory.class);
          AbstractPageRankExampleBlockFactory.NUM_ITERATIONS.set(conf, 100);

          SyntheticGraphInit.NUM_VERTICES.set(conf, 50000);
          SyntheticGraphInit.NUM_EDGES_PER_VERTEX.set(conf, 100);
          SyntheticGraphInit.NUM_COMMUNITIES.set(conf, 1000);
        });
  }
}
