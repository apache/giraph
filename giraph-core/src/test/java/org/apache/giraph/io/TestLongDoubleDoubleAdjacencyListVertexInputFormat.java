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


import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.LongDoubleDoubleAdjacencyListVertexInputFormat;

import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import static org.apache.giraph.io.TestTextDoubleDoubleAdjacencyListVertexInputFormat.assertValidVertex;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLongDoubleDoubleAdjacencyListVertexInputFormat extends LongDoubleDoubleAdjacencyListVertexInputFormat {

  private RecordReader<LongWritable, Text> rr;
  private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable,
      DoubleWritable> conf;
  private TaskAttemptContext tac;

  @Before
  public void setUp() throws IOException, InterruptedException {
    rr = mock(RecordReader.class);
    when(rr.nextKeyValue()).thenReturn(true);
    GiraphConfiguration giraphConf = new GiraphConfiguration();
    giraphConf.setComputationClass(DummyComputation.class);
    conf = new ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable,
        DoubleWritable>(giraphConf);
    tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);
  }

  protected TextVertexReader createVertexReader(
       RecordReader<LongWritable, Text> rr) {
    return createVertexReader(rr, null);
  }

  protected TextVertexReader createVertexReader(
      final RecordReader<LongWritable, Text> rr, LineSanitizer lineSanitizer) {
    return new LongDoubleDoubleAdjacencyListVertexReader(lineSanitizer) {
      @Override
      protected RecordReader<LongWritable, Text> createLineRecordReader(
          InputSplit inputSplit, TaskAttemptContext context)
          throws IOException, InterruptedException {
        return rr;
      }
    };
  }

  @Test
  public void testIndexMustHaveValue() throws IOException, InterruptedException {
    String input = "123";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextVertexReader vr = createVertexReader(rr);

    vr.setConf(conf);
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
    TextVertexReader vr = createVertexReader(rr);

    vr.setConf(conf);
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
    TextVertexReader vr = createVertexReader(rr);
    vr.setConf(conf);
    vr.initialize(null, tac);

    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<LongWritable, DoubleWritable, DoubleWritable>
        vertex = vr.getCurrentVertex();
    assertValidVertex(conf, vertex,
        new LongWritable(42), new DoubleWritable(0.1),
        EdgeFactory.create(new LongWritable(99), new DoubleWritable(0.2)),
        EdgeFactory.create(new LongWritable(2000), new DoubleWritable(0.3)),
        EdgeFactory.create(new LongWritable(4000), new DoubleWritable(0.4)));
    assertEquals(vertex.getNumEdges(), 3);
  }

  @Test
  public void testDifferentSeparators() throws Exception {
    String input = "12345:42.42:9999999:99.9";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    conf.set(AdjacencyListTextVertexInputFormat.LINE_TOKENIZE_VALUE, ":");
    TextVertexReader vr = createVertexReader(rr);
    vr.setConf(conf);
    vr.initialize(null, tac);
    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<LongWritable, DoubleWritable, DoubleWritable>
        vertex = vr.getCurrentVertex();
    assertValidVertex(conf, vertex,
        new LongWritable(12345), new DoubleWritable(42.42),
       EdgeFactory.create(new LongWritable(9999999), new DoubleWritable(99.9)));
    assertEquals(vertex.getNumEdges(), 1);
  }

  public static class DummyComputation extends NoOpComputation<LongWritable,
      DoubleWritable, DoubleWritable, BooleanWritable> { }
}
