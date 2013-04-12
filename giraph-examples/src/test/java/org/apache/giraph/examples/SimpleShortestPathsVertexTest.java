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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.giraph.examples.SimpleShortestPathsVertex.SOURCE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Contains a simple unit test for {@link SimpleShortestPathsVertex}
 */
public class SimpleShortestPathsVertexTest {

  /**
   * Test the behavior when a shorter path to a vertex has been found
   */
  @Test
  public void testOnShorterPathFound() throws Exception {
    SimpleShortestPathsVertex vertex = new SimpleShortestPathsVertex();

    MockUtils.MockedEnvironment<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> env = MockUtils.prepareVertex(vertex, 1L,
        new LongWritable(7L), new DoubleWritable(Double.MAX_VALUE), false);

    Mockito.when(SOURCE_ID.get(env.getConfiguration())).thenReturn(2L);

    vertex.addEdge(EdgeFactory.create(
        new LongWritable(10L), new FloatWritable(2.5f)));
    vertex.addEdge(EdgeFactory.create(
        new LongWritable(20L), new FloatWritable(0.5f)));

    vertex.compute(Lists.newArrayList(new DoubleWritable(2),
        new DoubleWritable(1.5)));

    assertTrue(vertex.isHalted());
    assertEquals(1.5d, vertex.getValue().get(), 0d);

    env.verifyMessageSent(new LongWritable(10L), new DoubleWritable(4));
    env.verifyMessageSent(new LongWritable(20L), new DoubleWritable(2));
  }

  /**
   * Test the behavior when a new, but not shorter path to a vertex has been
   * found.
   */
  @Test
  public void testOnNoShorterPathFound() throws Exception {

    SimpleShortestPathsVertex vertex = new SimpleShortestPathsVertex();

    MockUtils.MockedEnvironment<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> env = MockUtils.prepareVertex(vertex, 1L,
        new LongWritable(7L), new DoubleWritable(0.5), false);

    Mockito.when(SOURCE_ID.get(env.getConfiguration())).thenReturn(2L);

    vertex.addEdge(EdgeFactory.create(new LongWritable(10L),
        new FloatWritable(2.5f)));
    vertex.addEdge(EdgeFactory.create(
        new LongWritable(20L), new FloatWritable(0.5f)));

    vertex.compute(Lists.newArrayList(new DoubleWritable(2),
        new DoubleWritable(1.5)));

    assertTrue(vertex.isHalted());
    assertEquals(0.5d, vertex.getValue().get(), 0d);

    env.verifyNoMessageSent();
  }

  /**
   * A local integration test on toy data
   */
  @Test
  public void testToyData() throws Exception {

    // a small four vertex graph
    String[] graph = new String[] {
        "[1,0,[[2,1],[3,3]]]",
        "[2,0,[[3,1],[4,10]]]",
        "[3,0,[[4,2]]]",
        "[4,0,[]]"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    // start from vertex 1
    SOURCE_ID.set(conf, 1);
    conf.setVertexClass(SimpleShortestPathsVertex.class);
    conf.setVertexEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(
        JsonLongDoubleFloatDoubleVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
        JsonLongDoubleFloatDoubleVertexOutputFormat.class);

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    Map<Long, Double> distances = parseDistances(results);

    // verify results
    assertNotNull(distances);
    assertEquals(4, (int) distances.size());
    assertEquals(0.0, (double) distances.get(1L), 0d);
    assertEquals(1.0, (double) distances.get(2L), 0d);
    assertEquals(2.0, (double) distances.get(3L), 0d);
    assertEquals(4.0, (double) distances.get(4L), 0d);
  }

  private Map<Long, Double> parseDistances(Iterable<String> results) {
    Map<Long, Double> distances =
        Maps.newHashMapWithExpectedSize(Iterables.size(results));
    for (String line : results) {
      try {
        JSONArray jsonVertex = new JSONArray(line);
        distances.put(jsonVertex.getLong(0), jsonVertex.getDouble(1));
      } catch (JSONException e) {
        throw new IllegalArgumentException(
            "Couldn't get vertex from line " + line, e);
      }
    }
    return distances;
  }
}
