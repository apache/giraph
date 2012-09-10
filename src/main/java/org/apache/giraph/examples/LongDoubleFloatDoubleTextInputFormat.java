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
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Simple text-based {@link org.apache.giraph.graph.VertexInputFormat} for
 * unweighted graphs with long ids. Each line consists of: vertex neighbor1
 * neighbor2 ...
 */
public class LongDoubleFloatDoubleTextInputFormat
    extends
    TextVertexInputFormat<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {

  @Override
  public VertexReader<LongWritable,
    DoubleWritable, FloatWritable, DoubleWritable>
  createVertexReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new LongDoubleFloatDoubleVertexReader(
        textInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader associated with {@link LongDoubleFloatDoubleTextInputFormat}.
   */
  public static class LongDoubleFloatDoubleVertexReader extends
    TextVertexInputFormat.TextVertexReader<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    /**
     * Constructor with the line reader.
     * @param lineReader Internal line reader.
     */
    public LongDoubleFloatDoubleVertexReader(
        RecordReader<LongWritable, Text> lineReader) {
      super(lineReader);
    }

    @Override
    public Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
        vertex = BspUtils.<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable>createVertex(getContext()
            .getConfiguration());

      String[] tokens =
          SEPARATOR.split(getRecordReader().getCurrentValue().toString());
      Map<LongWritable, FloatWritable> edges =
          Maps.newHashMapWithExpectedSize(tokens.length - 1);
      float weight = 1.0f / (tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.put(new LongWritable(Long.parseLong(tokens[n])),
            new FloatWritable(weight));
      }

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, new DoubleWritable(), edges,
          Lists.<DoubleWritable>newArrayList());

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
