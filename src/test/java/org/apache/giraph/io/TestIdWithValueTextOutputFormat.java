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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.IdWithValueTextOutputFormat.IdWithValueVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Matchers;

import static org.apache.giraph.io.IdWithValueTextOutputFormat.IdWithValueVertexWriter.LINE_TOKENIZE_VALUE;
import static org.apache.giraph.io.IdWithValueTextOutputFormat.IdWithValueVertexWriter.REVERSE_ID_AND_VALUE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;

public class TestIdWithValueTextOutputFormat {
  @Test
  public void testHappyPath() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    Text expected = new Text("Four Tops\t4.0");

    IdWithValueTestWorker(conf, expected);
  }

  @Test
  public void testReverseIdAndValue() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setBoolean(REVERSE_ID_AND_VALUE, true);
    Text expected = new Text("4.0\tFour Tops");

    IdWithValueTestWorker(conf, expected);
  }

  @Test
  public void testWithDifferentDelimiter()  throws IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    conf.set(LINE_TOKENIZE_VALUE, "blah");
    Text expected = new Text("Four Topsblah4.0");

    IdWithValueTestWorker(conf, expected);
  }

  private void IdWithValueTestWorker(Configuration conf, Text expected)
      throws IOException, InterruptedException {
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Vertex vertex = mock(Vertex.class);
    when(vertex.getId()).thenReturn(new Text("Four Tops"));
    when(vertex.getValue()).thenReturn(new DoubleWritable(4d));

    // Create empty iterator == no edges
    when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

    RecordWriter<Text, Text> tw = mock(RecordWriter.class);
    IdWithValueVertexWriter writer = new IdWithValueVertexWriter(tw);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    verify(tw).write(expected, null);
    verify(vertex, times(0)).getEdges();
    verify(vertex, times(0)).getEdgeValue(Matchers.<WritableComparable>any());
  }
}
