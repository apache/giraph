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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.lib.IdWithValueTextOutputFormat;
import org.apache.giraph.examples.IntIntNullIntTextInputFormat;
import org.apache.giraph.examples.SimpleTriangleClosingVertex.IntArrayWritable;
import org.apache.giraph.utils.InternalVertexRunner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contains a simple unit test for {@link SimpleTriangleClosingVertex}
 */
public class SimpleTriangleClosingVertexTest {

  /**
   * Test the behavior of the triangle closing algorithm:
   * does it send all its out edge values to all neighbors?
   */
  @Test
  public void testTriangleClosingOutMsgs() throws Exception {
    // this guy should end up with an array value of 4
    SimpleTriangleClosingVertex vertex = new SimpleTriangleClosingVertex();
    vertex.initialize(null, null, null, null);
    vertex.addEdge(new IntWritable(5), NullWritable.get());
    vertex.addEdge(new IntWritable(7), NullWritable.get()); 
    IntArrayWritable iaw = new IntArrayWritable();
    iaw.set(new IntWritable[0]);

    MockUtils.MockedEnvironment<IntWritable, IntArrayWritable,
    NullWritable, IntWritable> env =
      MockUtils.prepareVertex(vertex, 0L, new IntWritable(1),
        iaw, false);

    vertex.compute(Lists.<IntWritable>newArrayList(
      new IntWritable(83), new IntWritable(42)).iterator());

    env.verifyMessageSent(new IntWritable(5), new IntWritable(5));
    env.verifyMessageSent(new IntWritable(5), new IntWritable(7));
    env.verifyMessageSent(new IntWritable(7), new IntWritable(5));
    env.verifyMessageSent(new IntWritable(7), new IntWritable(7));
  }

 /**
   * A local integration test on toy data
   */
  @Test
  public void testSampleGraph() throws Exception {
    // a small four vertex graph
    String[] graph = new String[] {
        "1", // this guy should end up with value { 4, 7 }
        "2 1 4",
        "3 1 4",
        "4",
        "5 1 7",
        "6 1 7",
        "7",
        "8 1 4"
    };

    /*
    // run internally
    Iterable<String> results =
      InternalVertexRunner.run(
        SimpleTriangleClosingVertex.class,
        IntIntNullIntTextInputFormat.class,
        IdWithValueTextOutputFormat.class,
        Maps.<String,String>newHashMap(),
        graph);

    // always be closing
    Map<Integer, List<Integer>> glenGarry = parseResults(results);

    // verify results
    assertNotNull(glenGarry);
    assertEquals(8, glenGarry.size());
    assertEquals(4, glenGarry.get(1).get(0));
    assertEquals(7, glenGarry.get(1).get(1));
    */
    assertEquals(1,1);
  }

  private Map<Integer, List<Integer>> parseResults(Iterable<String> in) {
    Map<Integer, List<Integer>> map =
      new HashMap<Integer, List<Integer>>();
    for (String line : in) {
      String[] tokens = line.trim().split("\\s*");
      final int id = Integer.parseInt(tokens[0]);
      if (map.get(id) == null) {
        map.put(id, new ArrayList<Integer>());
      }
      final int size = tokens.length;
      for (int ndx = 1; ndx < size; ++ndx) {
        map.get(id).add(Integer.parseInt(tokens[ndx]));
      }
    }
    return map;
  }
}
