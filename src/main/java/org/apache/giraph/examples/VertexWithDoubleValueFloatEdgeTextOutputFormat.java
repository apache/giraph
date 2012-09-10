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

package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple vertex output format for weighted graphs.
 */
public class VertexWithDoubleValueFloatEdgeTextOutputFormat extends
    TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
  @Override
  public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
  createVertexWriter(TaskAttemptContext context) throws IOException,
          InterruptedException {
    RecordWriter<Text, Text> recordWriter =
        textOutputFormat.getRecordWriter(context);
    return new VertexWithDoubleValueWriter(recordWriter);
  }

  /**
   * Vertex writer used with {@link VertexWithComponentTextOutputFormat}.
   */
  public static class VertexWithDoubleValueWriter extends
    TextVertexOutputFormat.TextVertexWriter<LongWritable,
    DoubleWritable, FloatWritable> {
    /**
     * Constructor with record writer.
     * @param writer Where the vertices will finally be written.
     */
    public VertexWithDoubleValueWriter(RecordWriter<Text, Text> writer) {
      super(writer);
    }

    @Override
    public void writeVertex(
        Vertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
      throws IOException, InterruptedException {
      StringBuilder output = new StringBuilder();
      output.append(vertex.getId().get());
      output.append('\t');
      output.append(vertex.getValue().get());
      getRecordWriter().write(new Text(output.toString()), null);
    }
  }
}
