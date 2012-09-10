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
 * unweighted graphs with long ids. Each line consists of: vertex
 * neighbor1:weight1 neighbor2:weight2 ...
 */
public class NormalizingLongDoubleFloatDoubleTextInputFormat
    extends
    TextVertexInputFormat<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> {

  @Override
  public VertexReader<LongWritable, DoubleWritable,
  FloatWritable, DoubleWritable> createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new NormalizingLongDoubleFloatDoubleVertexReader(
        textInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader associated with {@link LongDoubleFloatDoubleTextInputFormat}.
   */
  public static class NormalizingLongDoubleFloatDoubleVertexReader
      extends
      TextVertexInputFormat.TextVertexReader<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern EDGE_SEPARATOR = Pattern.compile("\\s+");
    /** Separator of the edge id and edge weight */
    private static final Pattern WEIGHT_SEPARATOR = Pattern.compile(":");

    /**
     * Constructor with the line reader.
     * @param lineReader
     *          Internal line reader.
     */
    public NormalizingLongDoubleFloatDoubleVertexReader(
        RecordReader<LongWritable, Text> lineReader) {
      super(lineReader);
    }

    @Override
    public Vertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> vertex = BspUtils
          .<LongWritable, DoubleWritable,
          FloatWritable, DoubleWritable>createVertex(getContext()
              .getConfiguration());

      String[] tokens = EDGE_SEPARATOR.split(getRecordReader()
          .getCurrentValue().toString());
      Map<LongWritable, FloatWritable> edges = Maps
          .newHashMapWithExpectedSize(tokens.length - 1);
      parse(tokens, edges);
      normalize(edges);

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, new DoubleWritable(), edges,
          Lists.<DoubleWritable>newArrayList());

      return vertex;
    }

    /**
     * Parse a set of tokens into a map ID -> weight.
     * @param tokens The tokens to be parsed.
     * @param edges The map that will contain the result of the parsing.
     */
    static void parse(String[] tokens, Map<LongWritable, FloatWritable> edges) {
      for (int n = 1; n < tokens.length; n++) {
        String[] parts = WEIGHT_SEPARATOR.split(tokens[n]);
        edges.put(new LongWritable(Long.parseLong(parts[0])),
            new FloatWritable(Float.parseFloat(parts[1])));
      }
    }

    /**
     * Normalize the edges with L1 normalization.
     * @param edges The edges to be normalized.
     */
    static void normalize(Map<LongWritable, FloatWritable> edges) {
      if (edges == null || edges.size() == 0) {
        throw new IllegalArgumentException(
            "Cannot normalize an empy set of edges");
      }
      float normalizer = 0.0f;
      for (FloatWritable weight : edges.values()) {
        normalizer += weight.get();
      }
      for (FloatWritable weight : edges.values()) {
        weight.set(weight.get() / normalizer);
      }
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
