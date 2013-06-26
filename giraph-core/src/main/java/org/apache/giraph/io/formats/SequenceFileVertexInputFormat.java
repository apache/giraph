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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Sequence file vertex input format based on {@link SequenceFileInputFormat}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <X> Value type
 */
@SuppressWarnings("rawtypes")
public class SequenceFileVertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable, X extends Vertex<I, V, E>>
    extends VertexInputFormat<I, V, E> {
  /** Internal input format */
  protected SequenceFileInputFormat<I, X> sequenceFileInputFormat =
    new SequenceFileInputFormat<I, X>();

  @Override public void checkInputSpecs(Configuration conf) { }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    return sequenceFileInputFormat.getSplits(context);
  }

  @Override
  public VertexReader<I, V, E> createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new SequenceFileVertexReader<I, V, E, X>(
        sequenceFileInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader used with {@link SequenceFileVertexInputFormat}.
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <X> Value type
   */
  public static class SequenceFileVertexReader<I extends WritableComparable,
      V extends Writable, E extends Writable, X extends Vertex<I, V, E>>
      extends VertexReader<I, V, E> {
    /** Internal record reader from {@link SequenceFileInputFormat} */
    private final RecordReader<I, X> recordReader;

    /**
     * Constructor with record reader.
     *
     * @param recordReader Reader from {@link SequenceFileInputFormat}.
     */
    public SequenceFileVertexReader(RecordReader<I, X> recordReader) {
      this.recordReader = recordReader;
    }

    @Override public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException, InterruptedException {
      recordReader.initialize(inputSplit, context);
    }

    @Override public boolean nextVertex() throws IOException,
        InterruptedException {
      return recordReader.nextKeyValue();
    }

    @Override public Vertex<I, V, E> getCurrentVertex()
      throws IOException, InterruptedException {
      return recordReader.getCurrentValue();
    }


    @Override public void close() throws IOException {
      recordReader.close();
    }

    @Override public float getProgress() throws IOException,
        InterruptedException {
      return recordReader.getProgress();
    }
  }
}
