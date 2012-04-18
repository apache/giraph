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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.lib.AdjacencyListTextVertexOutputFormat.AdjacencyListVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Matchers;

public class TestAdjacencyListTextVertexOutputFormat {

  @Test
  public void testVertexWithNoEdges() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    BasicVertex vertex = mock(BasicVertex.class);
    when(vertex.getVertexId()).thenReturn(new Text("The Beautiful South"));
    when(vertex.getVertexValue()).thenReturn(new DoubleWritable(32.2d));
    // Create empty iterator == no edges
    when(vertex.iterator()).thenReturn(new ArrayList<Text>().iterator());

    RecordWriter<Text, Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("The Beautiful South\t32.2");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).iterator();
    verify(vertex, times(0)).getEdgeValue(Matchers.<WritableComparable>any());
  }

  @Test
  public void testVertexWithEdges() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    BasicVertex vertex = mock(BasicVertex.class);
    when(vertex.getVertexId()).thenReturn(new Text("San Francisco"));
    when(vertex.getVertexValue()).thenReturn(new DoubleWritable(0d));
    when(vertex.getNumEdges()).thenReturn(2l);
    ArrayList<Text> cities = new ArrayList<Text>();
    Collections.addAll(cities, new Text("Los Angeles"), new Text("Phoenix"));

    when(vertex.iterator()).thenReturn(cities.iterator());
    mockEdgeValue(vertex, "Los Angeles", 347.16);
    mockEdgeValue(vertex, "Phoenix", 652.48);

    RecordWriter<Text,Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("San Francisco\t0.0\tLos Angeles\t347.16\t" +
            "Phoenix\t652.48");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).iterator();
    verify(vertex, times(2)).getEdgeValue(Matchers.<WritableComparable>any());
  }

  @Test
  public void testWithDifferentDelimiter() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(AdjacencyListVertexWriter.LINE_TOKENIZE_VALUE, ":::");
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    BasicVertex vertex = mock(BasicVertex.class);
    when(vertex.getVertexId()).thenReturn(new Text("San Francisco"));
    when(vertex.getVertexValue()).thenReturn(new DoubleWritable(0d));
    when(vertex.getNumEdges()).thenReturn(2l);
    ArrayList<Text> cities = new ArrayList<Text>();
    Collections.addAll(cities, new Text("Los Angeles"), new Text("Phoenix"));

    when(vertex.iterator()).thenReturn(cities.iterator());
    mockEdgeValue(vertex, "Los Angeles", 347.16);
    mockEdgeValue(vertex, "Phoenix", 652.48);

    RecordWriter<Text,Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("San Francisco:::0.0:::Los Angeles:::347.16:::" +
            "Phoenix:::652.48");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).iterator();
    verify(vertex, times(2)).getEdgeValue(Matchers.<WritableComparable>any());
  }

  private void mockEdgeValue(BasicVertex vertex, String s, double d) {
    when(vertex.getEdgeValue(new Text(s))).thenReturn(new DoubleWritable(d));
  }
}
