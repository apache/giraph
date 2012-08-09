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


import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import static org.apache.giraph.io.TestTextDoubleDoubleAdjacencyListVertexInputFormat.assertValidVertex;
import static org.apache.giraph.io.TestTextDoubleDoubleAdjacencyListVertexInputFormat.setGraphState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

public class TestLongDoubleDoubleAdjacencyListVertexInputFormat {

  private RecordReader<LongWritable, Text> rr;
  private Configuration conf;
  private TaskAttemptContext tac;
  private GraphState<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable> graphState;

  @Before
  public void setUp() throws IOException, InterruptedException {
    rr = mock(RecordReader.class);
    when(rr.nextKeyValue()).thenReturn(true);
    conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_CLASS, DummyVertex.class, Vertex.class);
    conf.setClass(GiraphJob.VERTEX_ID_CLASS, LongWritable.class, Writable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, DoubleWritable.class, Writable.class);
    graphState = mock(GraphState.class);
    tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);
  }

  @Test
  public void testIndexMustHaveValue() throws IOException, InterruptedException {
    String input = "123";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable> vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable>(rr);

    vr.initialize(null, tac);

    try {
      vr.nextVertex();
      vr.getCurrentVertex();
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  @Test
  public void testEdgesMustHaveValues() throws IOException, InterruptedException {
    String input = "99\t55.2\t100";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);

    try {
      vr.nextVertex();
      vr.getCurrentVertex();
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  @Test
  public void testHappyPath() throws Exception {
    String input = "42\t0.1\t99\t0.2\t2000\t0.3\t4000\t0.4";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable> vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable>(rr);

    vr.initialize(null, tac);

    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
        vertex = vr.getCurrentVertex();
    setGraphState(vertex, graphState);
    assertValidVertex(conf, graphState, vertex,
        new LongWritable(42), new DoubleWritable(0.1),
        new Edge<LongWritable, DoubleWritable>(new LongWritable(99), new DoubleWritable(0.2)),
        new Edge<LongWritable, DoubleWritable>(new LongWritable(2000), new DoubleWritable(0.3)),
        new Edge<LongWritable, DoubleWritable>(new LongWritable(4000), new DoubleWritable(0.4)));
    assertEquals(vertex.getNumEdges(), 3);
  }

  @Test
  public void testDifferentSeparators() throws Exception {
    String input = "12345:42.42:9999999:99.9";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    conf.set(AdjacencyListVertexReader.LINE_TOKENIZE_VALUE, ":");
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable> vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader<BooleanWritable>(rr);

    vr.initialize(null, tac);
    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
        vertex = vr.getCurrentVertex();
    setGraphState(vertex, graphState);
    assertValidVertex(conf, graphState, vertex, new LongWritable(12345), new DoubleWritable(42.42),
       new Edge<LongWritable, DoubleWritable>(new LongWritable(9999999), new DoubleWritable(99.9)));
    assertEquals(vertex.getNumEdges(), 1);
  }

  public static class DummyVertex
      extends EdgeListVertex<LongWritable, DoubleWritable,
      DoubleWritable, BooleanWritable> {
    @Override
    public void compute(Iterable<BooleanWritable> messages) throws IOException {
      // ignore
    }
  }
}
