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

import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexValueInputFormat}
 * for integer ids and values.
 *
 * Each line consists of: id, value
 *
 * @param <E> Edge value
 */
public class IntIntTextVertexValueInputFormat<E extends Writable> extends
    TextVertexValueInputFormat<IntWritable, IntWritable, E> {
  /** Separator for id and value */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexValueReader createVertexValueReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new IntIntTextVertexValueReader();
  }

  /**
   * {@link org.apache.giraph.io.VertexValueReader} associated with
   * {@link IntIntTextVertexValueInputFormat}.
   */
  public class IntIntTextVertexValueReader extends
      TextVertexValueReaderFromEachLineProcessed<IntPair> {

    @Override
    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new IntPair(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
    }

    @Override
    protected IntWritable getId(IntPair data) throws IOException {
      return new IntWritable(data.getFirst());
    }

    @Override
    protected IntWritable getValue(IntPair data) throws IOException {
      return new IntWritable(data.getSecond());
    }
  }
}
