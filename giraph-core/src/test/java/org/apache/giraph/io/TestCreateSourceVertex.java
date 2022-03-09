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

package org.apache.giraph.io;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.DefaultCreateSourceVertexCallback;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntNullTextVertexInputFormat;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.utils.ComputationCountEdges;
import org.apache.giraph.utils.IntIntNullNoOpComputation;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test createSourceVertex configuration option
 */
public class TestCreateSourceVertex {
  @Test
  public void testPositiveCreateSourceVertex() throws Exception {
    String [] vertices = new String[] {
        "1 0",
        "2 0",
        "3 0",
        "4 0",
    };
    String [] edges = new String[] {
        "1 2",
        "1 5",
        "2 4",
        "2 1",
        "3 4",
        "4 1",
        "4 5",
        "6 2",
        "7 8",
        "4 8",
    };

    GiraphConfiguration conf = getConf();
    conf.setCreateSourceVertex(false);

    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
    Map<Integer, Integer> values = parseResults(results);

    // Check that only vertices from vertex input are present in output graph
    assertEquals(4, values.size());
    // Check that the ids of vertices in output graph exactly match vertex input
    assertTrue(values.containsKey(1));
    assertTrue(values.containsKey(2));
    assertTrue(values.containsKey(3));
    assertTrue(values.containsKey(4));

    conf.setComputationClass(ComputationCountEdges.class);
    results = InternalVertexRunner.run(conf, vertices, edges);
    values = parseResults(results);

    // Check the number of edges of each vertex
    assertEquals(2, (int) values.get(1));
    assertEquals(2, (int) values.get(2));
    assertEquals(1, (int) values.get(3));
    assertEquals(3, (int) values.get(4));
  }

  @Test
  public void testNegativeCreateSourceVertex() throws Exception {
    String [] vertices = new String[] {
        "1 0",
        "2 0",
        "3 0",
        "4 0",
    };
    String [] edges = new String[] {
        "1 2",
        "1 5",
        "2 4",
        "2 1",
        "3 4",
        "4 1",
        "4 5",
        "6 2",
        "7 8",
        "4 8",
    };

    GiraphConfiguration conf = getConf();

    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
    Map<Integer, Integer> values = parseResults(results);

    // Check that only vertices from vertex input are present in output graph
    assertEquals(6, values.size());
    // Check that the ids of vertices in output graph exactly match vertex input
    assertTrue(values.containsKey(1));
    assertTrue(values.containsKey(2));
    assertTrue(values.containsKey(3));
    assertTrue(values.containsKey(4));
    assertTrue(values.containsKey(6));
    assertTrue(values.containsKey(7));

    conf.setComputationClass(ComputationCountEdges.class);
    results = InternalVertexRunner.run(conf, vertices, edges);
    values = parseResults(results);

    // Check the number of edges of each vertex
    assertEquals(2, (int) values.get(1));
    assertEquals(2, (int) values.get(2));
    assertEquals(1, (int) values.get(3));
    assertEquals(3, (int) values.get(4));
    assertEquals(1, (int) values.get(6));
    assertEquals(1, (int) values.get(7));
  }

  @Test
  public void testCustomCreateSourceVertex() throws Exception {
    String [] vertices = new String[] {
        "1 0",
        "2 0",
        "3 0",
        "4 0",
    };
    String [] edges = new String[] {
        "1 2",
        "1 5",
        "2 4",
        "2 1",
        "3 4",
        "4 1",
        "4 5",
        "6 2",
        "7 8",
        "4 8",
    };

    GiraphConfiguration conf = getConf();
    GiraphConstants.CREATE_EDGE_SOURCE_VERTICES_CALLBACK.set(conf,
        CreateEvenSourceVerticesCallback.class);

    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
    Map<Integer, Integer> values = parseResults(results);

    // Check that only vertices from vertex input are present in output graph
    assertEquals(5, values.size());
    // Check that the ids of vertices in output graph exactly match vertex input
    assertTrue(values.containsKey(1));
    assertTrue(values.containsKey(2));
    assertTrue(values.containsKey(3));
    assertTrue(values.containsKey(4));
    assertTrue(values.containsKey(6));

    conf.setComputationClass(ComputationCountEdges.class);
    results = InternalVertexRunner.run(conf, vertices, edges);
    values = parseResults(results);

    // Check the number of edges of each vertex
    assertEquals(2, (int) values.get(1));
    assertEquals(2, (int) values.get(2));
    assertEquals(1, (int) values.get(3));
    assertEquals(3, (int) values.get(4));
    assertEquals(1, (int) values.get(6));
  }

  /**
   * Only allows to create vertices with even ids.
   */
  public static class CreateEvenSourceVerticesCallback extends
      DefaultCreateSourceVertexCallback<IntWritable> {

    @Override
    public boolean shouldCreateSourceVertex(IntWritable vertexId) {
      return vertexId.get()  % 2 == 0;
    }
  }


  private GiraphConfiguration getConf() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(IntIntNullNoOpComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(IntIntNullTextVertexInputFormat.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    return conf;
  }

  private static Map<Integer, Integer> parseResults(Iterable<String> results) {
    Map<Integer, Integer> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      int value = Integer.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }
}
