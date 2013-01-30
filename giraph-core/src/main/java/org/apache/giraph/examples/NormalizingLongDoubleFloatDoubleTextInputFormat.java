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
import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.vertex.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with long ids. Each line consists of: vertex
 * neighbor1:weight1 neighbor2:weight2 ...
 */
public class NormalizingLongDoubleFloatDoubleTextInputFormat
    extends
    TextVertexInputFormat<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> {

  @Override
  public TextVertexReader createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new NormalizingLongDoubleFloatDoubleVertexReader();
  }

  /**
   * Vertex reader associated with {@link LongDoubleFloatDoubleTextInputFormat}.
   */
  public class NormalizingLongDoubleFloatDoubleVertexReader
      extends TextVertexInputFormat.TextVertexReader {
    /** Separator of the vertex and neighbors */
    private final Pattern edgeSeparator = Pattern.compile("\\s+");
    /** Separator of the edge id and edge weight */
    private final Pattern weightSeparator = Pattern.compile(":");

    @Override
    public Vertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> vertex = BspUtils
          .<LongWritable, DoubleWritable,
          FloatWritable, DoubleWritable>createVertex(getContext()
              .getConfiguration());

      String[] tokens = edgeSeparator.split(getRecordReader()
          .getCurrentValue().toString());
      List<Edge<LongWritable, FloatWritable>> edges = Lists
          .newArrayListWithCapacity(tokens.length - 1);
      parse(tokens, edges);
      normalize(edges);

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, new DoubleWritable(), edges);

      return vertex;
    }

    /**
     * Parse a set of tokens into a map ID -> weight.
     * @param tokens The tokens to be parsed.
     * @param edges The map that will contain the result of the parsing.
     */
    void parse(String[] tokens,
               Collection<Edge<LongWritable, FloatWritable>> edges) {
      for (int n = 1; n < tokens.length; n++) {
        String[] parts = weightSeparator.split(tokens[n]);
        edges.add(new DefaultEdge<LongWritable, FloatWritable>(
            new LongWritable(Long.parseLong(parts[0])),
            new FloatWritable(Float.parseFloat(parts[1]))));
      }
    }

    /**
     * Normalize the edges with L1 normalization.
     * @param edges The edges to be normalized.
     */
    void normalize(Collection<Edge<LongWritable, FloatWritable>> edges) {
      if (edges == null || edges.size() == 0) {
        throw new IllegalArgumentException(
            "Cannot normalize an empy set of edges");
      }
      float normalizer = 0.0f;
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        normalizer += edge.getValue().get();
      }
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        edge.getValue().set(edge.getValue().get() / normalizer);
      }
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
