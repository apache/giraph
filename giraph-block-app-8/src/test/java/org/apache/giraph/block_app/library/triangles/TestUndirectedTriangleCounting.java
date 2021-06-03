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

package org.apache.giraph.block_app.library.triangles;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link UndirectedTriangleCountingBlockFactory}
 */
public class TestUndirectedTriangleCounting {
  @Test
  public void smallTest() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    BlockUtils.setAndInitBlockFactoryClass(
        conf, UndirectedTriangleCountingBlockFactory.class);
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph =
        new NumericTestGraph<>(conf);

    graph.addSymmetricEdge(1, 4);
    graph.addSymmetricEdge(1, 5);
    graph.addSymmetricEdge(1, 6);
    graph.addSymmetricEdge(2, 4);
    graph.addSymmetricEdge(2, 6);
    graph.addSymmetricEdge(3, 6);
    graph.addSymmetricEdge(4, 5);
    graph.addSymmetricEdge(5, 6);

    graph.addSymmetricEdge(7, 8);
    graph.addSymmetricEdge(7, 9);
    graph.addSymmetricEdge(8, 9);

    graph.addSymmetricEdge(10, 11);
    graph.addSymmetricEdge(10, 12);
    graph.addSymmetricEdge(10, 13);
    graph.addSymmetricEdge(10, 14);
    graph.addSymmetricEdge(11, 12);
    graph.addSymmetricEdge(11, 13);
    graph.addSymmetricEdge(12, 13);
    graph.addSymmetricEdge(13, 14);

    LocalBlockRunner.runApp(graph.getTestGraph());

    assertEquals(graph.getValue(1).get(), 2);
    assertEquals(graph.getValue(2).get(), 0);
    assertEquals(graph.getValue(3).get(), 0);
    assertEquals(graph.getValue(4).get(), 1);
    assertEquals(graph.getValue(5).get(), 2);
    assertEquals(graph.getValue(6).get(), 1);
    assertEquals(graph.getValue(7).get(), 1);
    assertEquals(graph.getValue(8).get(), 1);
    assertEquals(graph.getValue(9).get(), 1);
    assertEquals(graph.getValue(10).get(), 4);
    assertEquals(graph.getValue(11).get(), 3);
    assertEquals(graph.getValue(12).get(), 3);
    assertEquals(graph.getValue(13).get(), 4);
    assertEquals(graph.getValue(14).get(), 1);
  }
}
