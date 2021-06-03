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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class IntNullTextEdgeInputFormat extends
    TextEdgeInputFormat<IntWritable, NullWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<IntWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new IntNullTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class IntNullTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<IntPair> {
    @Override
    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new IntPair(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
    }

    @Override
    protected IntWritable getSourceVertexId(IntPair endpoints)
      throws IOException {
      return new IntWritable(endpoints.getFirst());
    }

    @Override
    protected IntWritable getTargetVertexId(IntPair endpoints)
      throws IOException {
      return new IntWritable(endpoints.getSecond());
    }

    @Override
    protected NullWritable getValue(IntPair endpoints) throws IOException {
      return NullWritable.get();
    }
  }
}
