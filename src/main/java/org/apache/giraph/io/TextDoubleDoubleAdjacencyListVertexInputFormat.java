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

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Class to read graphs stored as adjacency lists with ids represented by
 * Strings and values as doubles.  This is a good inputformat for reading
 * graphs where the id types do not matter and can be stashed in a String.
 *
 * @param <M> Message type.
 */
public class TextDoubleDoubleAdjacencyListVertexInputFormat<M extends Writable>
    extends TextVertexInputFormat<Text, DoubleWritable, DoubleWritable, M>  {


  /**
   * Vertex reader used with
   * {@link TextDoubleDoubleAdjacencyListVertexInputFormat}
   *
   * @param <M> Message type.
   */
  static class VertexReader<M extends Writable> extends
      AdjacencyListVertexReader<Text, DoubleWritable, DoubleWritable, M> {
    /**
     * Constructor without sanitzer.
     *
     * @param lineRecordReader Internal reader.
     */
    VertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
      super(lineRecordReader);
    }

    /**
     * Constructor with {@link LineRecordReader}
     *
     * @param lineRecordReader Internal reader.
     * @param sanitizer Sanitizer of the lines.
     */
    VertexReader(RecordReader<LongWritable, Text> lineRecordReader,
        LineSanitizer sanitizer) {
      super(lineRecordReader, sanitizer);
    }

    @Override
    public void decodeId(String s, Text id) {
      id.set(s);
    }

    @Override
    public void decodeValue(String s, DoubleWritable value) {
      value.set(Double.valueOf(s));
    }

    @Override
    public void decodeEdge(String s1, String s2,
                           Edge<Text, DoubleWritable> textIntWritableEdge) {
      textIntWritableEdge.setTargetVertexId(new Text(s1));
      textIntWritableEdge.setValue(new DoubleWritable(Double.valueOf(s2)));
    }
  }

  @Override
  public org.apache.giraph.graph.VertexReader<Text, DoubleWritable,
      DoubleWritable, M> createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new VertexReader<M>(textInputFormat.createRecordReader(
        split, context));
  }
}
