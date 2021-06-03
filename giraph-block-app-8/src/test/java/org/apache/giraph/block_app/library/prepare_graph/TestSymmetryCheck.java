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

import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.block_app.test_setup.graphs.SmallDirectedTreeGraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.function.ObjectHolder;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.utils.hashing.LongWritableFunnel;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSymmetryCheck {
  private long IGNORED = -1L;

  private NumericTestGraph<LongWritable, LongWritable, LongWritable> graph;
  private ObjectHolder<Boolean> holder = new ObjectHolder<>();
  private Block isBlock = PrepareGraphPieces.isSymmetricBlock(LongWritableFunnel.INSTANCE, holder);

  @Before
  public void initConf() {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, LongWritable.class);

    graph = new NumericTestGraph<>(conf);
  }

  @Test
  public void testSimpleLoop() throws Exception {
    graph.addVertex(0l, IGNORED, 0l);

    LocalBlockRunner.runBlock(graph.getTestGraph(), isBlock, new Object());

    assertTrue(holder.get());
  }

  @Test
  public void testSimpleAsymmetric() throws Exception {
    graph.addVertex(0l, IGNORED, IGNORED, 1l);
    graph.addVertex(1l, IGNORED);

    LocalBlockRunner.runBlock(graph.getTestGraph(), isBlock, new Object());

    assertFalse(holder.get());
  }

  @Test
  public void testSimpleSymmetric() throws Exception {
    graph.addVertex(0l, IGNORED, IGNORED, 1l);
    graph.addVertex(1l, IGNORED, IGNORED, 0l);

    LocalBlockRunner.runBlock(graph.getTestGraph(), isBlock, new Object());

    assertTrue(holder.get());
  }

  @Test
  public void testSmall1Graph() throws Exception {
    Small1GraphInit i = new Small1GraphInit<LongWritable, LongWritable, LongWritable>();
    i.modifyGraph(graph);

    LocalBlockRunner.runBlock(graph.getTestGraph(), isBlock, new Object());

    assertTrue(holder.get());
  }

  @Test
  public void testSmallDirectedTreeAsymmetric() throws Exception {
    SmallDirectedTreeGraphInit i = new SmallDirectedTreeGraphInit<LongWritable, LongWritable, LongWritable>();
    i.modifyGraph(graph);

    LocalBlockRunner.runBlock(graph.getTestGraph(), isBlock, new Object());

    assertFalse(holder.get());
  }

  @Test
  public void testSmallDirectedTreeSymmetric() throws Exception {
    SmallDirectedTreeGraphInit i = new SmallDirectedTreeGraphInit<LongWritable, LongWritable, LongWritable>();
    i.modifyGraph(graph);

    Block block =
        new SequenceBlock(PrepareGraphPieces.makeSymmetricWeighted(LongTypeOps.INSTANCE, LongTypeOps.INSTANCE),
            isBlock);
    LocalBlockRunner.runBlock(graph.getTestGraph(), block, new Object());

    assertTrue(holder.get());
  }
}
