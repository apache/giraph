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
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.block_app.library.prepare_graph.vertex.ConnectedComponentVertexValue;
import org.apache.giraph.block_app.library.prepare_graph.vertex.WeaklyConnectedComponentVertexValue;
import org.apache.giraph.block_app.library.prepare_graph.vertex.WeaklyConnectedComponentVertexValueImpl;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.block_app.test_setup.graphs.Small2GraphInit;
import org.apache.giraph.block_app.test_setup.graphs.SmallDirectedForestGraphInit;
import org.apache.giraph.block_app.test_setup.graphs.SmallDirectedTreeGraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class TestConnectedComponents {

  private static final
  SupplierFromVertex<LongWritable, LongWritable, Writable, LongWritable> GET_COMPONENT =
    VertexSuppliers.<LongWritable, LongWritable, Writable>vertexValueSupplier();

  private static final
  ConsumerWithVertex<LongWritable, LongWritable, Writable, LongWritable> SET_COMPONENT =
    (vertex, value) -> vertex.getValue().set(value.get());


  private <V extends Writable>
  NumericTestGraph<LongWritable, V, NullWritable> createEmptyGraph(Class<V> vertexValue) {
    GiraphConfiguration conf = new GiraphConfiguration();

    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, vertexValue);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, NullWritable.class);

    NumericTestGraph<LongWritable, V, NullWritable> graph = new NumericTestGraph<>(conf);
    return graph;
  }

  private NumericTestGraph<LongWritable, LongWritable, NullWritable> createEmptyGraph() {
    return createEmptyGraph(LongWritable.class);
  }

  @Test
  public void testCC() throws Exception {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createEmptyGraph();
    new Small1GraphInit<LongWritable, LongWritable, NullWritable>().modifyGraph(graph);

    Block ccBlock = UndirectedConnectedComponents.calculateConnectedComponents(
        100, GET_COMPONENT, SET_COMPONENT);
    LocalBlockRunner.runBlock(graph.getTestGraph(), ccBlock, new Object());

    for (int i : new int[] {0, 1, 2, 3, 4, 5}) {
      Assert.assertEquals(0, graph.getValue(i).get());
    }
    for (int i : new int[] {6}) {
      Assert.assertEquals(6, graph.getValue(i).get());
    }

    Block keepLargestBlock = UndirectedConnectedComponents.calculateAndKeepLargestComponent(
        100, GET_COMPONENT, SET_COMPONENT);

    LocalBlockRunner.runBlock(graph.getTestGraph(), keepLargestBlock, new Object());

    for (int i : new int[] {0, 1, 2, 3, 4, 5}) {
      Assert.assertEquals(0, graph.getValue(i).get());
    }

    for (int i : new int[] {6}) {
      Assert.assertNull(graph.getVertex(i));
    }
  }

  @Test
  public void testMultipleComponentCC() throws Exception {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createEmptyGraph();
    new Small2GraphInit<LongWritable, LongWritable, NullWritable>().modifyGraph(graph);

    Block ccBlock = UndirectedConnectedComponents.calculateConnectedComponents(
        100, GET_COMPONENT, SET_COMPONENT);
    LocalBlockRunner.runBlock(graph.getTestGraph(), ccBlock, new Object());

    for (int i : new int[] {0, 1, 2}) {
      Assert.assertEquals(0, graph.getValue(i).get());
    }
    for (int i : new int[] {3, 4, 5}) {
      Assert.assertEquals(3, graph.getValue(i).get());
    }
    for (int i : new int[] {6}) {
      Assert.assertEquals(6, graph.getValue(i).get());
    }

    Block keepAbove3 = UndirectedConnectedComponents.calculateAndKeepComponentAboveThreshold(
        100, 3, GET_COMPONENT, SET_COMPONENT);

    LocalBlockRunner.runBlock(graph.getTestGraph(), keepAbove3, new Object());

    for (int i : new int[] {0, 1, 2}) {
      Assert.assertEquals(0, graph.getValue(i).get());
    }
    for (int i : new int[] {3, 4, 5}) {
      Assert.assertEquals(3, graph.getValue(i).get());
    }

    for (int i : new int[] {6}) {
      Assert.assertNull(graph.getVertex(i));
    }

    Block keepAbove4 = UndirectedConnectedComponents.calculateAndKeepComponentAboveThreshold(
        100, 4, GET_COMPONENT, SET_COMPONENT);

    LocalBlockRunner.runBlock(graph.getTestGraph(), keepAbove4, new Object());

    for (int i : new int[] {0, 1, 2, 3, 4, 5}) {
      Assert.assertNull(graph.getVertex(i));
    }
  }

  @Test
  public void testWeaklyCCOnTree() {
    NumericTestGraph<LongWritable, WeaklyConnectedComponentVertexValueImpl, NullWritable> graph =
        createEmptyGraph(WeaklyConnectedComponentVertexValueImpl.class);
    new SmallDirectedTreeGraphInit<LongWritable, WeaklyConnectedComponentVertexValueImpl, NullWritable>().modifyGraph(graph);

    Block weaklyCC = WeaklyConnectedComponents.calculateConnectedComponents(
        200,
        ConnectedComponentVertexValue.getComponentSupplier(),
        ConnectedComponentVertexValue.setComponentConsumer(),
        WeaklyConnectedComponentVertexValue.getEdgeIdsSupplier(),
        WeaklyConnectedComponentVertexValue.setEdgeIdsConsumer(),
        false);

    LocalBlockRunner.runBlock(graph.getTestGraph(), weaklyCC, new Object());

    for (int i : new int[] {0, 1, 2, 3, 4, 5, 6}) {
      Assert.assertEquals(0, graph.getValue(i).getComponent());
    }
  }

  @Test
  public void testWeaklyCCOnForest() {
    NumericTestGraph<LongWritable, WeaklyConnectedComponentVertexValueImpl, NullWritable> graph =
        createEmptyGraph(WeaklyConnectedComponentVertexValueImpl.class);
    new SmallDirectedForestGraphInit<LongWritable, WeaklyConnectedComponentVertexValueImpl, NullWritable>().modifyGraph(graph);

    Block weaklyCC = WeaklyConnectedComponents.calculateConnectedComponents(
        200,
        ConnectedComponentVertexValue.getComponentSupplier(),
        ConnectedComponentVertexValue.setComponentConsumer(),
        WeaklyConnectedComponentVertexValue.getEdgeIdsSupplier(),
        WeaklyConnectedComponentVertexValue.setEdgeIdsConsumer(),
        false);

    LocalBlockRunner.runBlock(graph.getTestGraph(), weaklyCC, new Object());

    for (int i : new int[] {0, 1, 2, 3}) {
      Assert.assertEquals(0, graph.getValue(i).getComponent());
    }

    for (int i : new int[] {4, 5}) {
      Assert.assertEquals(4, graph.getValue(i).getComponent());
    }

    for (int i : new int[] {6, 7, 8}) {
      Assert.assertEquals(6, graph.getValue(i).getComponent());
    }
  }
}
