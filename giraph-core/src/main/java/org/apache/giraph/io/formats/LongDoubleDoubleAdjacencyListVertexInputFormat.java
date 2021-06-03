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
package org.apache.giraph.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat for reading graphs stored as (ordered) adjacency lists
 * with the vertex ids longs and the vertex values and edges doubles.
 * For example:
 * 22 0.1 45 0.3 99 0.44
 * to repesent a vertex with id 22, value of 0.1 and edges to nodes 45 and 99,
 * with values of 0.3 and 0.44, respectively.
 */
public class LongDoubleDoubleAdjacencyListVertexInputFormat
    extends AdjacencyListTextVertexInputFormat<LongWritable, DoubleWritable,
    DoubleWritable> {

  @Override
  public AdjacencyListTextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new LongDoubleDoubleAdjacencyListVertexReader(null);
  }

  /**
   * VertexReader associated with
   * {@link LongDoubleDoubleAdjacencyListVertexInputFormat}.
   */
  protected class LongDoubleDoubleAdjacencyListVertexReader extends
      AdjacencyListTextVertexReader {

    /**
     * Constructor with
     * {@link AdjacencyListTextVertexInputFormat.LineSanitizer}.
     *
     * @param lineSanitizer the sanitizer to use for reading
     */
    public LongDoubleDoubleAdjacencyListVertexReader(LineSanitizer
        lineSanitizer) {
      super(lineSanitizer);
    }

    @Override
    public LongWritable decodeId(String s) {
      return new LongWritable(Long.parseLong(s));
    }

    @Override
    public DoubleWritable decodeValue(String s) {
      return new DoubleWritable(Double.parseDouble(s));
    }

    @Override
    public Edge<LongWritable, DoubleWritable> decodeEdge(String s1, String s2) {
      return EdgeFactory.create(new LongWritable(Long.parseLong(s1)),
          new DoubleWritable(Double.parseDouble(s2)));
    }
  }

}
