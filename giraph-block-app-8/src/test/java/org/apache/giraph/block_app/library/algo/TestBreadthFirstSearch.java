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
package org.apache.giraph.block_app.library.algo;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class TestBreadthFirstSearch {
  private void run(
    TestGraphModifier<LongWritable, BreadthFirstSearchVertexValue, NullWritable>
      graphLoader,
    int[] expectedDistances,
    int[] seedVertices
  ) throws Exception {
    TestGraphModifier<LongWritable, BreadthFirstSearchVertexValue, NullWritable>
      valueLoader =
      (graph) -> {
        List<Integer> seeds = Arrays.asList(ArrayUtils.toObject(seedVertices));
        for (int i = 0; i < graph.getVertexCount(); i++)
          graph.getVertex(i).getValue().setSeedVertex(seeds.contains(i));
      };

    TestGraphUtils.runTest(
      TestGraphUtils.chainModifiers(graphLoader, valueLoader),
      (graph) -> {
        for (int i = 0; i < expectedDistances.length; i++) {
          assertEquals(expectedDistances[i], graph.getValue(i).getDistance());
        }
      },
      (conf) -> {
        BlockUtils.setBlockFactoryClass(conf,
          BreadthFirstSearchBlockFactory.class);
      }
    );
  }

  @Test
  public void testSmall1SingleSeed() throws Exception {
    int[] expected = {0, 1, 1, 2, 3, 3, -1};
    int[] seeds = {0};
    run(new Small1GraphInit<>(), expected, seeds);
  }

  @Test
  public void testSmall1TwoSeeds() throws Exception {
    int[] expected = {0, 1, 1, 1, 0, 1, -1};
    int[] seeds = {0, 4};
    run(new Small1GraphInit<>(), expected, seeds);
  }

  @Test
  public void testSmall1IsolatedSeed() throws Exception {
    int[] expected = {-1, -1, -1, -1, -1, -1, 0};
    int[] seeds = {6};
    run(new Small1GraphInit<>(), expected, seeds);
  }

  @Test
  public void testSmallGraphTwoSeeds() throws Exception {
    int[] expected = {0, 1, 2, 2, 2, 2, 3, 4, 5, 5, 5, 1, 2, 2, 2, 0};
    int[] seeds = {0, 15};
    run(new Graph1Init<>(), expected, seeds);
  }

  @Test
  public void testSmallGraphTwoCloseSeeds() throws Exception {
    int[] expected = {1, 0, 1, 0, 1, 1, 1, 2, 3, 3, 3, 2, 3, 3, 3, 3};
    int[] seeds = {1, 3};
    run(new Graph1Init<>(), expected, seeds);
  }

  @Test
  public void testMultipleComponentGraphCloseSeeds() throws Exception {
    int[] expected =
      {2, 1, 0, 1, 2, 3, 3, 3, 2, 2, 2, 2, 1, 0, 2, -1, -1, -1, -1, -1, -1};
    int[] seeds = {13, 2};
    run(new Graph2Init(), expected, seeds);
  }

  @Test
  public void testMultipleComponentGraphFarSeeds() throws Exception {
    int[] expected =
      {3, 2, 3, 2, 1, 0, 1, 2, 1, 2, 3, 3, 2, 3, 3, 3, 2, 1, 0, 1, -1};
    int[] seeds = {5, 18};
    run(new Graph2Init(), expected, seeds);
  }


  public class Graph1Init<I extends WritableComparable, V extends Writable,
    E extends Writable> implements TestGraphModifier<I, V, E> {

    @Override
    public void modifyGraph(NumericTestGraph<I, V, E> graph) {
      graph.addVertex(0, (Number) null, null, 1);
      graph.addVertex(1, (Number) null, null, 0,2,3,4,5);
      graph.addVertex(2, (Number) null, null, 1,3,4,5);
      graph.addVertex(3, (Number) null, null, 1,2,4,5,6);
      graph.addVertex(4, (Number) null, null, 1,2,3,5);
      graph.addVertex(5, (Number) null, null, 1,2,3,4,11);
      graph.addVertex(6, (Number) null, null, 3,7);
      graph.addVertex(7, (Number) null, null, 6,8,9,10);
      graph.addVertex(8, (Number) null, null, 7,9,10);
      graph.addVertex(9, (Number) null, null, 7,8,10);
      graph.addVertex(10, (Number) null, null, 7,8,9);
      graph.addVertex(11, (Number) null, null, 5,12,13,14,15);
      graph.addVertex(12, (Number) null, null, 11);
      graph.addVertex(13, (Number) null, null, 11);
      graph.addVertex(14, (Number) null, null, 11);
      graph.addVertex(15, (Number) null, null, 11);
    }
  }

  public class Graph2Init<I extends WritableComparable, V extends Writable,
    E extends Writable> implements TestGraphModifier<I, V, E> {

    @Override
    public void modifyGraph(NumericTestGraph<I, V, E> graph) {
      graph.addVertex(0, (Number) null, null, 1);
      graph.addVertex(1, (Number) null, null, 0,2,3,4);
      graph.addVertex(2, (Number) null, null, 1,3);
      graph.addVertex(3, (Number) null, null, 2,4,9,10,11);
      graph.addVertex(4, (Number) null, null, 1,3,5,6,7);
      graph.addVertex(5, (Number) null, null, 4,6,5,8);
      graph.addVertex(6, (Number) null, null, 4,5,7);
      graph.addVertex(7, (Number) null, null, 4,5,6);
      graph.addVertex(8, (Number) null, null, 5,9,12);
      graph.addVertex(9, (Number) null, null, 3,8,10,11,12);
      graph.addVertex(10, (Number) null, null, 3,9,11);
      graph.addVertex(11, (Number) null, null, 3,9,10);
      graph.addVertex(12, (Number) null, null, 8,9,13,14);
      graph.addVertex(13, (Number) null, null, 12);
      graph.addVertex(14, (Number) null, null, 12);
      graph.addVertex(15, (Number) null, null, 16);
      graph.addVertex(16, (Number) null, null, 15,17,19);
      graph.addVertex(17, (Number) null, null, 16,18);
      graph.addVertex(18, (Number) null, null, 17,19);
      graph.addVertex(19, (Number) null, null, 16,18);
      graph.addVertex(20);
    }
  }
}
