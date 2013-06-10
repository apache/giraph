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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTextDoubleDoubleAdjacencyListVertexInputFormat extends TextDoubleDoubleAdjacencyListVertexInputFormat {

  private RecordReader<LongWritable, Text> rr;
  private ImmutableClassesGiraphConfiguration<Text, DoubleWritable,
      DoubleWritable> conf;
  private TaskAttemptContext tac;

  @Before
  public void setUp() throws IOException, InterruptedException {
    rr = mock(RecordReader.class);
    when(rr.nextKeyValue()).thenReturn(true).thenReturn(false);
    GiraphConfiguration giraphConf = new GiraphConfiguration();
    giraphConf.setComputationClass(DummyComputation.class);
    conf = new ImmutableClassesGiraphConfiguration<Text, DoubleWritable,
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
    return new TextDoubleDoubleAdjacencyListVertexReader(lineSanitizer) {
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
    String input = "hi";

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
    String input = "index\t55.66\tindex2";

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

  public static <I extends WritableComparable, V extends Writable,
      E extends WritableComparable> void assertValidVertex(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Vertex<I, V, E> actual,
      I expectedId,
      V expectedValue,
      Edge<I, E>... edges) throws Exception {
    Vertex<I, V, E> expected = conf.createVertex();
    expected.initialize(expectedId, expectedValue, Arrays.asList(edges));
    assertValid(expected, actual);
  }

  public static <I extends WritableComparable, V extends Writable,
      E extends WritableComparable> void assertValid(
      Vertex<I, V, E> expected, Vertex<I, V, E> actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getValue(), actual.getValue());
    assertTrue(EdgeIterables.equals(expected.getEdges(), actual.getEdges()));
  }

  @Test
  public void testHappyPath() throws Exception {
    String input = "Hi\t0\tCiao\t1.123\tBomdia\t2.234\tOla\t3.345";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextVertexReader vr = createVertexReader(rr);
    vr.setConf(conf);
    vr.initialize(null, tac);
    assertTrue("Should have been able to add a vertex", vr.nextVertex());
    Vertex<Text, DoubleWritable, DoubleWritable> vertex = vr.getCurrentVertex();
    assertValidVertex(conf, vertex,
        new Text("Hi"), new DoubleWritable(0),
        EdgeFactory.create(new Text("Ciao"), new DoubleWritable(1.123d)),
        EdgeFactory.create(new Text("Bomdia"), new DoubleWritable(2.234d)),
        EdgeFactory.create(new Text("Ola"), new DoubleWritable(3.345d)));
    assertEquals(vertex.getNumEdges(), 3);
  }

  @Test
  public void testLineSanitizer() throws Exception {
    String input = "Bye\t0.01\tCiao\t1.001\tTchau\t2.0001\tAdios\t3.00001";

    AdjacencyListTextVertexInputFormat.LineSanitizer toUpper =
        new AdjacencyListTextVertexInputFormat.LineSanitizer() {
      @Override
      public String sanitize(String s) {
        return s.toUpperCase();
      }
    };

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextVertexReader vr = createVertexReader(rr, toUpper);
    vr.setConf(conf);
    vr.initialize(null, tac);
    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<Text, DoubleWritable, DoubleWritable> vertex = vr.getCurrentVertex();
    assertValidVertex(conf, vertex,
        new Text("BYE"), new DoubleWritable(0.01d),
        EdgeFactory.create(new Text("CIAO"), new DoubleWritable(1.001d)),
        EdgeFactory.create(new Text("TCHAU"), new DoubleWritable(2.0001d)),
        EdgeFactory.create(new Text("ADIOS"), new DoubleWritable(3.00001d)));

    assertEquals(vertex.getNumEdges(), 3);
  }

  @Test
  public void testDifferentSeparators() throws Exception {
    String input = "alpha:42:beta:99";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    conf.set(AdjacencyListTextVertexInputFormat.LINE_TOKENIZE_VALUE, ":");
    TextVertexReader vr = createVertexReader(rr);
    vr.setConf(conf);
    vr.initialize(null, tac);
    assertTrue("Should have been able to read vertex", vr.nextVertex());
    Vertex<Text, DoubleWritable, DoubleWritable> vertex = vr.getCurrentVertex();
    assertValidVertex(conf, vertex,
        new Text("alpha"), new DoubleWritable(42d),
        EdgeFactory.create(new Text("beta"), new DoubleWritable(99d)));
    assertEquals(vertex.getNumEdges(), 1);
  }

  public static class DummyComputation extends NoOpComputation<Text,
      DoubleWritable, DoubleWritable, BooleanWritable> { }
}
