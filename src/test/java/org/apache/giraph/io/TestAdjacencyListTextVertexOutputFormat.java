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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.AdjacencyListTextVertexOutputFormat.AdjacencyListVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Matchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class TestAdjacencyListTextVertexOutputFormat {

  @Test
  public void testVertexWithNoEdges() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Vertex vertex = mock(Vertex.class);
    when(vertex.getId()).thenReturn(new Text("The Beautiful South"));
    when(vertex.getValue()).thenReturn(new DoubleWritable(32.2d));
    // Create empty iterable == no edges
    when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

    RecordWriter<Text, Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("The Beautiful South\t32.2");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).getEdges();
    verify(vertex, times(0)).getEdgeValue(Matchers.<WritableComparable>any());
  }

  @Test
  public void testVertexWithEdges() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Vertex vertex = mock(Vertex.class);
    when(vertex.getId()).thenReturn(new Text("San Francisco"));
    when(vertex.getValue()).thenReturn(new DoubleWritable(0d));
    when(vertex.getTotalNumEdges()).thenReturn(2l);
    ArrayList<Edge<Text, DoubleWritable>> cities = new ArrayList<Edge<Text,
        DoubleWritable>>();
    Collections.addAll(cities,
        new Edge<Text, DoubleWritable>(
            new Text("Los Angeles"), new DoubleWritable(347.16)),
        new Edge<Text, DoubleWritable>(
            new Text("Phoenix"), new DoubleWritable(652.48)));

    when(vertex.getEdges()).thenReturn(cities);

    RecordWriter<Text,Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("San Francisco\t0.0\tLos Angeles\t347.16\t" +
            "Phoenix\t652.48");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).getEdges();
  }

  @Test
  public void testWithDifferentDelimiter() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(AdjacencyListVertexWriter.LINE_TOKENIZE_VALUE, ":::");
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Vertex vertex = mock(Vertex.class);
    when(vertex.getId()).thenReturn(new Text("San Francisco"));
    when(vertex.getValue()).thenReturn(new DoubleWritable(0d));
    when(vertex.getTotalNumEdges()).thenReturn(2l);
    ArrayList<Edge<Text, DoubleWritable>> cities = new ArrayList<Edge<Text,
        DoubleWritable>>();
    Collections.addAll(cities,
        new Edge<Text, DoubleWritable>(
            new Text("Los Angeles"), new DoubleWritable(347.16)),
        new Edge<Text, DoubleWritable>(
            new Text("Phoenix"), new DoubleWritable(652.48)));

    when(vertex.getEdges()).thenReturn(cities);

    RecordWriter<Text,Text> tw = mock(RecordWriter.class);
    AdjacencyListVertexWriter writer = new AdjacencyListVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    Text expected = new Text("San Francisco:::0.0:::Los Angeles:::347.16:::" +
            "Phoenix:::652.48");
    verify(tw).write(expected, null);
    verify(vertex, times(1)).getEdges();
  }
}
