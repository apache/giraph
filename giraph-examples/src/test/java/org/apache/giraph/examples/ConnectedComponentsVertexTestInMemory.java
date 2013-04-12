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

package org.apache.giraph.examples;

import org.apache.giraph.combiner.MinimumIntCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  Tests for {@link ConnectedComponentsVertex}
 */
public class ConnectedComponentsVertexTestInMemory {
  public static Entry<IntWritable, NullWritable>[] makeEdges(int... args){
    Entry<IntWritable, NullWritable> result[] =
      new Entry[args.length];
    for (int i=0; i<args.length; i++){
      result[i] = new SimpleEntry<IntWritable, NullWritable>(
          new IntWritable(args[i]), NullWritable.get());
    }
    return result;
  }
  /**
   * A local integration test on toy data
   */
  @Test
  public void testToyData() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(ConnectedComponentsVertex.class);
    conf.setVertexEdgesClass(ByteArrayEdges.class);
    conf.setCombinerClass(MinimumIntCombiner.class);

    TestGraph<IntWritable, IntWritable, NullWritable, IntWritable> graph =
      new TestGraph<IntWritable, IntWritable, NullWritable, IntWritable> (conf);
    // a small graph with three components
    graph.addVertex(new IntWritable(1), new IntWritable(1), makeEdges(2, 3))
      .addVertex(new IntWritable(2), new IntWritable(2), makeEdges(1, 4, 5))
      .addVertex(new IntWritable(3), new IntWritable(3), makeEdges(1, 4))
      .addVertex(new IntWritable(4), new IntWritable(4),
          makeEdges(2, 3, 5, 13))
      .addVertex(new IntWritable(5), new IntWritable(5),
          makeEdges(2, 4, 12, 13))
      .addVertex(new IntWritable(12), new IntWritable(12), makeEdges(5, 13))
      .addVertex(new IntWritable(13), new IntWritable(13), makeEdges(4, 5, 12))
      .addVertex(new IntWritable(6), new IntWritable(6), makeEdges(7, 8))
      .addVertex(new IntWritable(7), new IntWritable(7), makeEdges(6, 10, 11))
      .addVertex(new IntWritable(8), new IntWritable(8), makeEdges(6, 10))
      .addVertex(new IntWritable(10), new IntWritable(10), makeEdges(7, 8, 11))
      .addVertex(new IntWritable(11), new IntWritable(11), makeEdges(7, 10))
      .addVertex(new IntWritable(9), new IntWritable(9));

    // run internally
    TestGraph<IntWritable, IntWritable, NullWritable, IntWritable> results =
      InternalVertexRunner.run(conf, graph);

    SetMultimap<Integer,Integer> components = parseResults(results);

    Set<Integer> componentIDs = components.keySet();
    assertEquals(3, componentIDs.size());
    assertTrue(componentIDs.contains(1));
    assertTrue(componentIDs.contains(6));
    assertTrue(componentIDs.contains(9));

    Set<Integer> componentOne = components.get(1);
    assertEquals(7, componentOne.size());
    assertTrue(componentOne.contains(1));
    assertTrue(componentOne.contains(2));
    assertTrue(componentOne.contains(3));
    assertTrue(componentOne.contains(4));
    assertTrue(componentOne.contains(5));
    assertTrue(componentOne.contains(12));
    assertTrue(componentOne.contains(13));

    Set<Integer> componentTwo = components.get(6);
    assertEquals(5, componentTwo.size());
    assertTrue(componentTwo.contains(6));
    assertTrue(componentTwo.contains(7));
    assertTrue(componentTwo.contains(8));
    assertTrue(componentTwo.contains(10));
    assertTrue(componentTwo.contains(11));

    Set<Integer> componentThree = components.get(9);
    assertEquals(1, componentThree.size());
    assertTrue(componentThree.contains(9));
  }

  private SetMultimap<Integer,Integer> parseResults(
    TestGraph<IntWritable, IntWritable, NullWritable, IntWritable> results) {
    SetMultimap<Integer,Integer> components = HashMultimap.create();
    for (Vertex<IntWritable,
                IntWritable,
                NullWritable,
                IntWritable> vertex: results) {
      int component = vertex.getValue().get();
      components.put(component, vertex.getId().get());
    }
    return components;
  }
}
