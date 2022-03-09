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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class to read graphs stored as adjacency lists with ids represented by
 * Strings and values as doubles.  This is a good inputformat for reading
 * graphs where the id types do not matter and can be stashed in a String.
 */
public class TextDoubleDoubleAdjacencyListVertexInputFormat
    extends AdjacencyListTextVertexInputFormat<Text, DoubleWritable,
            DoubleWritable>  {

  @Override
  public AdjacencyListTextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new TextDoubleDoubleAdjacencyListVertexReader(null);
  }

  /**
   * Vertex reader used with
   * {@link TextDoubleDoubleAdjacencyListVertexInputFormat}
   */
  protected class TextDoubleDoubleAdjacencyListVertexReader extends
      AdjacencyListTextVertexReader {

    /**
     * Constructor with
     * {@link AdjacencyListTextVertexInputFormat.LineSanitizer}.
     *
     * @param lineSanitizer the sanitizer to use for reading
     */
    public TextDoubleDoubleAdjacencyListVertexReader(LineSanitizer
        lineSanitizer) {
      super(lineSanitizer);
    }

    @Override
    public Text decodeId(String s) {
      return new Text(s);
    }

    @Override
    public DoubleWritable decodeValue(String s) {
      return new DoubleWritable(Double.parseDouble(s));
    }

    @Override
    public Edge<Text, DoubleWritable> decodeEdge(String s1, String s2) {
      return EdgeFactory.create(new Text(s1),
          new DoubleWritable(Double.parseDouble(s2)));
    }
  }

}
