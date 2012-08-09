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

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.graph.VertexInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: vertex neighbor1 neighbor2 ...
 */
public class IntIntNullIntTextInputFormat extends
    TextVertexInputFormat<IntWritable, IntWritable, NullWritable,
    IntWritable> {

  @Override
  public VertexReader<IntWritable, IntWritable, NullWritable, IntWritable>
  createVertexReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new IntIntNullIntVertexReader(
        textInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader associated with {@link IntIntNullIntTextInputFormat}.
   */
  public static class IntIntNullIntVertexReader extends
      TextVertexInputFormat.TextVertexReader<IntWritable, IntWritable,
      NullWritable, IntWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    /**
     * Constructor with the line reader.
     *
     * @param lineReader Internal line reader.
     */
    public IntIntNullIntVertexReader(RecordReader<LongWritable, Text>
    lineReader) {
      super(lineReader);
    }

    @Override
    public Vertex<IntWritable, IntWritable, NullWritable, IntWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<IntWritable, IntWritable, NullWritable, IntWritable>
      vertex = BspUtils.<IntWritable, IntWritable, NullWritable,
      IntWritable>createVertex(getContext().getConfiguration());

      String[] tokens = SEPARATOR.split(getRecordReader()
          .getCurrentValue().toString());
      Map<IntWritable, NullWritable> edges =
          Maps.newHashMapWithExpectedSize(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.put(new IntWritable(Integer.parseInt(tokens[n])),
            NullWritable.get());
      }

      IntWritable vertexId = new IntWritable(Integer.parseInt(tokens[0]));
      vertex.initialize(vertexId, vertexId, edges,
          Lists.<IntWritable>newArrayList());

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
