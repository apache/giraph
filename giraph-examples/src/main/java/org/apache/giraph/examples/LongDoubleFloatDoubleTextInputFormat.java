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

import org.apache.giraph.bsp.BspUtils;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.vertex.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with long ids. Each line consists of: vertex neighbor1
 * neighbor2 ...
 */
public class LongDoubleFloatDoubleTextInputFormat
    extends
    TextVertexInputFormat<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new LongDoubleFloatDoubleVertexReader();
  }

  /**
   * Vertex reader associated with {@link LongDoubleFloatDoubleTextInputFormat}.
   */
  public class LongDoubleFloatDoubleVertexReader extends
    TextVertexInputFormat.TextVertexReader {
    /** Separator of the vertex and neighbors */
    private final Pattern separator = Pattern.compile("[\t ]");

    @Override
    public Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
        vertex = BspUtils.<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable>createVertex(getContext()
            .getConfiguration());

      String[] tokens =
          separator.split(getRecordReader().getCurrentValue().toString());
      List<Edge<LongWritable, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      float weight = 1.0f / (tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.add(EdgeFactory.create(
            new LongWritable(Long.parseLong(tokens[n])),
            new FloatWritable(weight)));
      }

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, new DoubleWritable(), edges);

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
