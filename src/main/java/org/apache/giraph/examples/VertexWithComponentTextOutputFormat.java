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

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Text-based {@link org.apache.giraph.graph.VertexOutputFormat} for usage with
 * {@link ConnectedComponentsVertex}
 *
 * Each line consists of a vertex and its associated component (represented
 *  by the smallest vertex id in the component)
 */
public class VertexWithComponentTextOutputFormat extends
    TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
  @Override
  public VertexWriter<IntWritable, IntWritable, NullWritable>
  createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    RecordWriter<Text, Text> recordWriter =
        textOutputFormat.getRecordWriter(context);
    return new VertexWithComponentWriter(recordWriter);
  }

  /**
   * Vertex writer used with {@link VertexWithComponentTextOutputFormat}.
   */
  public static class VertexWithComponentWriter extends
      TextVertexOutputFormat.TextVertexWriter<IntWritable, IntWritable,
      NullWritable> {
    /**
     * Constructor with record writer.
     *
     * @param writer Where the vertices will finally be written.
     */
    public VertexWithComponentWriter(RecordWriter<Text, Text> writer) {
      super(writer);
    }

    @Override
    public void writeVertex(BasicVertex<IntWritable, IntWritable,
        NullWritable, ?> vertex) throws IOException,
        InterruptedException {
      StringBuilder output = new StringBuilder();
      output.append(vertex.getVertexId().get());
      output.append('\t');
      output.append(vertex.getVertexValue().get());
      getRecordWriter().write(new Text(output.toString()), null);
    }
  }
}
