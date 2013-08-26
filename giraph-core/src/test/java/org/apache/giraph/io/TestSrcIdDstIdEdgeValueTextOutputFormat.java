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

import static org.apache.giraph.conf.GiraphConstants.GIRAPH_TEXT_OUTPUT_FORMAT_REVERSE;
import static org.apache.giraph.conf.GiraphConstants.GIRAPH_TEXT_OUTPUT_FORMAT_SEPARATOR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

public class TestSrcIdDstIdEdgeValueTextOutputFormat
  extends SrcIdDstIdEdgeValueTextOutputFormat<LongWritable,
          LongWritable, LongWritable> {
  /** Test configuration */
  private ImmutableClassesGiraphConfiguration<
      LongWritable, LongWritable, LongWritable> conf;
  /**
   * Dummy class to allow ImmutableClassesGiraphConfiguration to be created.
   */
  public static class DummyComputation extends NoOpComputation<Text,
      DoubleWritable, DoubleWritable, DoubleWritable> { }

  @Before
  public void setUp() {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setComputationClass(DummyComputation.class);
    conf =
      new ImmutableClassesGiraphConfiguration<LongWritable, LongWritable,
        LongWritable>(giraphConfiguration);
  }

  @Test
  public void testHappyPath() throws IOException, InterruptedException {
    Text expected = new Text("0\t1\t5");

    checkSrcIdDstIdEdgeValueWorker(expected);
  }

  @Test
  public void testReverseIdAndValue() throws IOException, InterruptedException {
    GIRAPH_TEXT_OUTPUT_FORMAT_REVERSE.set(this.conf, true);
    Text expected = new Text("5\t1\t0");

    checkSrcIdDstIdEdgeValueWorker(expected);
  }

  @Test
  public void testWithDifferentDelimiter()  throws IOException,
      InterruptedException {
    GIRAPH_TEXT_OUTPUT_FORMAT_SEPARATOR.set(this.conf, "->");
    Text expected = new Text("0->1->5");

    checkSrcIdDstIdEdgeValueWorker(expected);
  }

  private void checkSrcIdDstIdEdgeValueWorker(Text expected)
    throws IOException, InterruptedException {

    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    when(tac.getConfiguration()).thenReturn(conf);

    Edge edge = mock(Edge.class);

    when(edge.getTargetVertexId()).thenReturn(new LongWritable(1));
    when(edge.getValue()).thenReturn(new LongWritable(5));

    final RecordWriter<Text, Text> tw = mock(RecordWriter.class);
    SrcIdDstIdEdgeValueEdgeWriter writer = new SrcIdDstIdEdgeValueEdgeWriter() {
      @Override
      protected RecordWriter<Text, Text> createLineRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {

        return tw;
      }
    };

    writer.setConf(conf);
    writer.initialize(tac);
    writer.writeEdge(new LongWritable(0), new LongWritable(0), edge);

    verify(tw).write(expected, null);
  }
}
