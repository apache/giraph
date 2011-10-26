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

public class TestLongDoubleDoubleAdjacencyListVertexInputFormat extends TestCase {

  private RecordReader<LongWritable, Text> rr;
  private Configuration conf;
  private TaskAttemptContext tac;

  public void setUp() throws IOException, InterruptedException {
    rr = mock(RecordReader.class);
    when(rr.nextKeyValue()).thenReturn(true);
    conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_INDEX_CLASS, LongWritable.class, Writable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, DoubleWritable.class, Writable.class);
    tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);
  }

  public void testIndexMustHaveValue() throws IOException, InterruptedException {
    String input = "123";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);

    try {
      vr.next(mutableVertex);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  public void testEdgesMustHaveValues() throws IOException, InterruptedException {
    String input = "99\t55.2\t100";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
            mutableVertex = mock(MutableVertex.class);
    try {
      vr.next(mutableVertex);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("Line did not split correctly: "));
    }
  }

  public void testHappyPath() throws IOException, InterruptedException {
    String input = "42\t0.1\t99\t0.2\t2000\t0.3\t4000\t0.4";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);

    assertTrue("Should have been able to read vertex", vr.next(mutableVertex));
    verify(mutableVertex).setVertexId(new LongWritable(42));
    verify(mutableVertex).setVertexValue(new DoubleWritable(0.1d));
    verify(mutableVertex).addEdge(new LongWritable(99l), new DoubleWritable(0.2d));
    verify(mutableVertex).addEdge(new LongWritable(2000l), new DoubleWritable(0.3d));
    verify(mutableVertex).addEdge(new LongWritable(4000l), new DoubleWritable(0.4d));
    verifyNoMoreInteractions(mutableVertex);
  }

  public void testDifferentSeparators() throws IOException, InterruptedException {
    String input = "12345:42.42:9999999:99.9";

    when(rr.getCurrentValue()).thenReturn(new Text(input));
    conf.set(AdjacencyListVertexReader.LINE_TOKENIZE_VALUE, ":");
    LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader vr =
        new LongDoubleDoubleAdjacencyListVertexInputFormat.VertexReader(rr);

    vr.initialize(null, tac);
    MutableVertex<LongWritable, DoubleWritable, DoubleWritable, BooleanWritable>
        mutableVertex = mock(MutableVertex.class);
    assertTrue("Should have been able to read vertex", vr.next(mutableVertex));
    verify(mutableVertex).setVertexId(new LongWritable(12345l));
    verify(mutableVertex).setVertexValue(new DoubleWritable(42.42d));
    verify(mutableVertex).addEdge(new LongWritable(9999999l),
        new DoubleWritable(99.9d));
    verifyNoMoreInteractions(mutableVertex);
  }

}
