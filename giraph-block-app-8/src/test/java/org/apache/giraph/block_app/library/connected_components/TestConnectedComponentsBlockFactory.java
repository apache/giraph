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

package org.apache.giraph.block_app.library.connected_components;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Connected components test
 */
public class TestConnectedComponentsBlockFactory {
  @Test
  public void testAlreadySymmetric() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    BlockUtils.setAndInitBlockFactoryClass(
        conf, ConnectedComponentsBlockFactory.class);
    ConnectedComponentsBlockFactory.MAKE_GRAPH_SYMMETRIC.set(conf, false);
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph =
        new NumericTestGraph<>(conf);

    graph.addSymmetricEdge(1, 2);
    graph.addSymmetricEdge(2, 3);
    graph.addSymmetricEdge(3, 0);
    graph.addSymmetricEdge(0, 5);
    graph.addSymmetricEdge(0, 6);
    graph.addSymmetricEdge(8, 9);

    LocalBlockRunner.runApp(graph.getTestGraph());

    assertEquals(graph.getValue(0).get(), 0);
    assertEquals(graph.getValue(1).get(), 0);
    assertEquals(graph.getValue(2).get(), 0);
    assertEquals(graph.getValue(3).get(), 0);
    assertEquals(graph.getValue(5).get(), 0);
    assertEquals(graph.getValue(6).get(), 0);
    assertEquals(graph.getValue(8).get(), 8);
    assertEquals(graph.getValue(9).get(), 8);
  }

  @Test
  public void testNotSymmetric() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    BlockUtils.setAndInitBlockFactoryClass(
        conf, ConnectedComponentsBlockFactory.class);
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph =
        new NumericTestGraph<>(conf);

    graph.addEdge(-2, 4);
    graph.addEdge(1, 2);
    graph.addEdge(1, 3);
    graph.addEdge(1, -2);
    graph.addEdge(3, 1);
    graph.addEdge(3, 4);
    graph.addEdge(4, 3);
    graph.addEdge(4, 13);
    graph.addEdge(12, 5);
    graph.addEdge(12, 13);
    graph.addEdge(13, 4);

    graph.addEdge(6, 7);
    graph.addEdge(7, 11);
    graph.addEdge(8, 6);
    graph.addEdge(10, 7);
    graph.addEdge(10, 11);

    graph.addVertex(9);

    LocalBlockRunner.runApp(graph.getTestGraph());

    assertEquals(graph.getValue(-2).get(), -2);
    assertEquals(graph.getValue(1).get(), -2);
    assertEquals(graph.getValue(2).get(), -2);
    assertEquals(graph.getValue(3).get(), -2);
    assertEquals(graph.getValue(4).get(), -2);
    assertEquals(graph.getValue(5).get(), -2);
    assertEquals(graph.getValue(6).get(), 6);
    assertEquals(graph.getValue(7).get(), 6);
    assertEquals(graph.getValue(8).get(), 6);
    assertEquals(graph.getValue(9).get(), 9);
    assertEquals(graph.getValue(10).get(), 6);
    assertEquals(graph.getValue(11).get(), 6);
    assertEquals(graph.getValue(12).get(), -2);
    assertEquals(graph.getValue(13).get(), -2);
  }
}
