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

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.io.GeneratedVertexInputFormat;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.giraph.io.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 */
public class SimpleSuperstepVertex extends
    EdgeListVertex<LongWritable, IntWritable, FloatWritable, IntWritable> {
  @Override
  public void compute(Iterable<IntWritable> messages) {
    if (getSuperstep() > 3) {
      voteToHalt();
    }
  }

  /**
   * Simple VertexReader that supports {@link SimpleSuperstepVertex}
   */
  public static class SimpleSuperstepVertexReader extends
      GeneratedVertexReader<LongWritable, IntWritable,
        FloatWritable, IntWritable> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SimpleSuperstepVertexReader.class);
    /**
     * Constructor.
     */
    public SimpleSuperstepVertexReader() {
      super();
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalRecords > recordsRead;
    }

    @Override
    public Vertex<LongWritable, IntWritable, FloatWritable,
        IntWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Vertex<LongWritable, IntWritable, FloatWritable, IntWritable> vertex =
        BspUtils.<LongWritable, IntWritable, FloatWritable,
        IntWritable>createVertex(configuration);
      long tmpId = reverseIdOrder ?
          ((inputSplit.getSplitIndex() + 1) * totalRecords) -
          recordsRead - 1 :
            (inputSplit.getSplitIndex() * totalRecords) + recordsRead;
      LongWritable vertexId = new LongWritable(tmpId);
      IntWritable vertexValue =
          new IntWritable((int) (vertexId.get() * 10));
      Map<LongWritable, FloatWritable> edgeMap = Maps.newHashMap();
      long targetVertexId =
          (vertexId.get() + 1) %
          (inputSplit.getNumSplits() * totalRecords);
      float edgeValue = vertexId.get() * 100f;
      edgeMap.put(new LongWritable(targetVertexId),
          new FloatWritable(edgeValue));
      vertex.initialize(vertexId, vertexValue, edgeMap, null);
      ++recordsRead;
      if (LOG.isInfoEnabled()) {
        LOG.info("next: Return vertexId=" + vertex.getId().get() +
            ", vertexValue=" + vertex.getValue() +
            ", targetVertexId=" + targetVertexId +
            ", edgeValue=" + edgeValue);
      }
      return vertex;
    }
  }

  /**
   * Simple VertexInputFormat that supports {@link SimpleSuperstepVertex}
   */
  public static class SimpleSuperstepVertexInputFormat extends
    GeneratedVertexInputFormat<LongWritable,
          IntWritable, FloatWritable, IntWritable> {
    @Override
    public VertexReader<LongWritable, IntWritable, FloatWritable, IntWritable>
    createVertexReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
      return new SimpleSuperstepVertexReader();
    }
  }

  /**
   * Simple VertexWriter that supports {@link SimpleSuperstepVertex}
   */
  public static class SimpleSuperstepVertexWriter extends
      TextVertexWriter<LongWritable, IntWritable, FloatWritable> {
    /**
     * Constructor with the line record writer.
     *
     * @param lineRecordWriter Writer to write to.
     */
    public SimpleSuperstepVertexWriter(
        RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
    }

    @Override
    public void writeVertex(Vertex<LongWritable, IntWritable,
            FloatWritable, ?> vertex) throws IOException, InterruptedException {
      getRecordWriter().write(
          new Text(vertex.getId().toString()),
          new Text(vertex.getValue().toString()));
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link SimpleSuperstepVertex}
   */
  public static class SimpleSuperstepVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, IntWritable, FloatWritable> {
    @Override
    public VertexWriter<LongWritable, IntWritable, FloatWritable>
    createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      RecordWriter<Text, Text> recordWriter =
          textOutputFormat.getRecordWriter(context);
      return new SimpleSuperstepVertexWriter(recordWriter);
    }
  }
}
