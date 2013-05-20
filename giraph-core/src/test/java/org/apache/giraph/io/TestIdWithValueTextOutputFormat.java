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
import java.util.ArrayList;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestIdWithValueTextOutputFormat extends
    IdWithValueTextOutputFormat<Text, DoubleWritable, Writable> {
  /** Test configuration */
  private ImmutableClassesGiraphConfiguration<
      Text, DoubleWritable, Writable> conf;
  /**
   * Dummy class to allow ImmutableClassesGiraphConfiguration to be created.
   */
  public static class DummyComputation extends NoOpComputation<Text,
      DoubleWritable, DoubleWritable, DoubleWritable> { }

  @Before
  public void setUp() {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setComputationClass(DummyComputation.class);
    conf = new ImmutableClassesGiraphConfiguration<Text, DoubleWritable,
        Writable>(giraphConfiguration);
  }

  @Test
  public void testHappyPath() throws IOException, InterruptedException {
    Text expected = new Text("Four Tops\t4.0");

    IdWithValueTestWorker(expected);
  }

  @Test
  public void testReverseIdAndValue() throws IOException, InterruptedException {
    conf.setBoolean(REVERSE_ID_AND_VALUE, true);
    Text expected = new Text("4.0\tFour Tops");

    IdWithValueTestWorker(expected);
  }

  @Test
  public void testWithDifferentDelimiter()  throws IOException,
      InterruptedException {
    conf.set(LINE_TOKENIZE_VALUE, "blah");
    Text expected = new Text("Four Topsblah4.0");

    IdWithValueTestWorker(expected);
  }

  private void IdWithValueTestWorker(Text expected)
      throws IOException, InterruptedException {
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Vertex vertex = mock(Vertex.class);
    when(vertex.getId()).thenReturn(new Text("Four Tops"));
    when(vertex.getValue()).thenReturn(new DoubleWritable(4d));

    // Create empty iterator == no edges
    when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

    final RecordWriter<Text, Text> tw = mock(RecordWriter.class);
    IdWithValueVertexWriter writer = new IdWithValueVertexWriter() {
      @Override
      protected RecordWriter<Text, Text> createLineRecordWriter(
          TaskAttemptContext context) throws IOException, InterruptedException {
        return tw;
      }
    };
    writer.setConf(conf);
    writer.initialize(tac);
    writer.writeVertex(vertex);

    verify(tw).write(expected, null);
    verify(vertex, times(0)).getEdges();
  }
}
