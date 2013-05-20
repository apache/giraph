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

import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntTextVertexValueInputFormat;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.utils.ComputationCountEdges;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.collect.Maps;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestFilters extends BspCase {
  public TestFilters() {
    super(TestFilters.class.getName());
  }

  public static class EdgeFilter implements EdgeInputFilter<IntWritable, NullWritable> {
    @Override public boolean dropEdge(IntWritable sourceId, Edge<IntWritable, NullWritable> edge) {
      return sourceId.get() == 2;
    }
  }

  @Test
  public void testEdgeFilter() throws Exception {
    String[] edges = new String[] {
        "1 2",
        "2 3",
        "2 4",
        "4 1"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(ComputationCountEdges.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setEdgeInputFilterClass(EdgeFilter.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, null, edges);

    Map<Integer, Integer> values = parseResults(results);

    assertEquals(2, values.size());
    assertEquals(1, (int) values.get(1));
    assertEquals(1, (int) values.get(4));
  }

  public static class VertexFilter implements VertexInputFilter<IntWritable,
      NullWritable, NullWritable> {
    @Override
    public boolean dropVertex(Vertex<IntWritable, NullWritable,
        NullWritable> vertex) {
      int id = vertex.getId().get();
      return id == 2 || id == 3;
    }
  }

  @Test
  public void testVertexFilter() throws Exception {
    String[] vertices = new String[] {
        "1 1",
        "2 2",
        "3 3",
        "4 4"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(IntNoOpComputation.class);
    conf.setVertexInputFormatClass(IntIntTextVertexValueInputFormat.class);
    conf.setVertexInputFilterClass(VertexFilter.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, vertices);

    Map<Integer, Integer> values = parseResults(results);

    assertEquals(2, values.size());
    assertEquals(1, (int) values.get(1));
    assertEquals(4, (int) values.get(4));
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
