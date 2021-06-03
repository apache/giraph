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

package org.apache.giraph.examples.scc;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SccComputation}
 */
public class SccComputationTestInMemory {
  @SuppressWarnings("unchecked")
  public static Entry<LongWritable, NullWritable>[] makeEdges(long... args) {
    Entry<LongWritable, NullWritable> result[] = new Entry[args.length];
    for (int i = 0; i < args.length; i++) {
      result[i] = new SimpleEntry<LongWritable, NullWritable>(new LongWritable(
          args[i]), NullWritable.get());
    }
    return result;
  }

  /**
   * Connects the outgoingVertices to the given vertex id
   * with null-valued edges.
   *
   * @param graph
   * @param id
   * @param outgoingVertices
   */
  public static void addVertex(
      TestGraph<LongWritable, SccVertexValue, NullWritable> graph, long id,
      long... outgoingVertices) {
    graph.addVertex(new LongWritable(id), new SccVertexValue(id),
        makeEdges(outgoingVertices));
  }

  @Test
  public void testToyData() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SccComputation.class);
    conf.setMasterComputeClass(SccPhaseMasterCompute.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);


    TestGraph<LongWritable, SccVertexValue, NullWritable> graph = new TestGraph<LongWritable, SccVertexValue, NullWritable>(
        conf);

    addVertex(graph, 0, 1, 2, 4);
    addVertex(graph, 1, 3, 20);
    addVertex(graph, 2, 3);
    addVertex(graph, 3, 0);
    addVertex(graph, 20, 21);
    addVertex(graph, 21, 22);
    addVertex(graph, 22, 23);
    addVertex(graph, 23, 24);
    addVertex(graph, 24, 25);
    addVertex(graph, 25, 20);
    addVertex(graph, 4, 5);
    addVertex(graph, 5, 6);

    TestGraph<LongWritable, SccVertexValue, NullWritable> results = InternalVertexRunner.runWithInMemoryOutput(conf, graph);

    Map<Long, List<Long>> scc = parse(results);

    List<Long> components = scc.get(3l);
    Collections.sort(components);
    Assert.assertEquals(Arrays.asList(0l, 1l, 2l, 3l), components);

    Assert.assertEquals(Arrays.asList(4l), scc.get(4l));
    Assert.assertEquals(Arrays.asList(5l), scc.get(5l));
    Assert.assertEquals(Arrays.asList(6l), scc.get(6l));

    components = scc.get(25l);
    Collections.sort(components);
    Assert.assertEquals(Arrays.asList(20l, 21l, 22l, 23l, 24l, 25l), components);
  }

  private Map<Long, List<Long>> parse(
      TestGraph<LongWritable, SccVertexValue, NullWritable> g) {
    Map<Long, List<Long>> scc = new HashMap<Long, List<Long>>();
    for (Vertex<LongWritable, SccVertexValue, NullWritable> vertex : g) {
      long sccId = vertex.getValue().get();
      List<Long> verticesIds = scc.get(sccId);
      if (verticesIds == null) {// New SCC
        List<Long> newScc = new ArrayList<Long>();
        newScc.add(vertex.getId().get());
        scc.put(sccId, newScc);
      } else {
        verticesIds.add(vertex.getId().get());
      }
    }
    return scc;
  }
}
