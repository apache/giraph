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
package org.apache.giraph.lib;


import junit.framework.TestCase;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestTextDoubleDoubleAdjacencyListVertexInputFormat extends TestCase {

  private RecordReader<LongWritable, Text> rr;
  private Configuration conf;
  private TaskAttemptContext tac;

  public void setUp() throws IOException, InterruptedException {
    rr = mock(RecordReader.class);
    when(rr.nextKeyValue()).thenReturn(true).thenReturn(false);
    conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_INDEX_CLASS, Text.class, Writable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, DoubleWritable.class, Writable.class);
    tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);
  }

  public void testIndexMustHaveValue() throws IOException, InterruptedException {
    String input = "hi";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<Text, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);

    try {
      vr.next(mutableVertex);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  public void testEdgesMustHaveValues() throws IOException, InterruptedException {
    String input = "index\t55.66\tindex2";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<Text, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);
    try {
      vr.next(mutableVertex);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  public void testHappyPath() throws IOException, InterruptedException {
    String input = "Hi\t0\tCiao\t1.123\tBomdia\t2.234\tOla\t3.345";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<Text, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);

    assertTrue("Should have been able to read vertex", vr.next(mutableVertex));
    verify(mutableVertex).setVertexId(new Text("Hi"));
    verify(mutableVertex).setVertexValue(new DoubleWritable(0));
    verify(mutableVertex).addEdge(new Text("Ciao"), new DoubleWritable(1.123d));
    verify(mutableVertex).addEdge(new Text("Bomdia"), new DoubleWritable(2.234d));
    verify(mutableVertex).addEdge(new Text("Ola"), new DoubleWritable(3.345d));
    verifyNoMoreInteractions(mutableVertex);
  }

  public void testLineSanitizer() throws IOException, InterruptedException {
    String input = "Bye\t0.01\tCiao\t1.001\tTchau\t2.0001\tAdios\t3.00001";

    AdjacencyListVertexReader.LineSanitizer toUpper =
        new AdjacencyListVertexReader.LineSanitizer() {
      @Override
      public String sanitize(String s) {
        return s.toUpperCase();
      }
    };

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr, toUpper);

    vr.initialize(null, tac);
    MutableVertex<Text, DoubleWritable, DoubleWritable, BooleanWritable>
            mutableVertex = mock(MutableVertex.class);
    assertTrue("Should have been able to read vertex", vr.next(mutableVertex));
    verify(mutableVertex).setVertexId(new Text("BYE"));
    verify(mutableVertex).setVertexValue(new DoubleWritable(0.01d));
    verify(mutableVertex).addEdge(new Text("CIAO"), new DoubleWritable(1.001d));
    verify(mutableVertex).addEdge(new Text("TCHAU"), new DoubleWritable(2.0001d));
    verify(mutableVertex).addEdge(new Text("ADIOS"), new DoubleWritable(3.00001d));
    verifyNoMoreInteractions(mutableVertex);
  }

  public void testDifferentSeparators() throws IOException, InterruptedException {
    String input = "alpha:42:beta:99";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    conf.set(AdjacencyListVertexReader.LINE_TOKENIZE_VALUE, ":");
    TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new TextDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<Text, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);
    assertTrue("Should have been able to read vertex", vr.next(mutableVertex));
    verify(mutableVertex).setVertexId(new Text("alpha"));
    verify(mutableVertex).setVertexValue(new DoubleWritable(42));
    verify(mutableVertex).addEdge(new Text("beta"), new DoubleWritable(99));
    verifyNoMoreInteractions(mutableVertex);
  }

}
