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

package org.apache.giraph.io.formats.multi;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.internal.WrappedVertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Vertex input format which wraps several vertex input formats.
 * Provides the way to read data from multiple sources,
 * using several different input formats.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class MultiVertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable> extends VertexInputFormat<I, V, E> {
  /** Vertex input formats */
  private List<VertexInputFormat<I, V, E>> vertexInputFormats;

  @Override public void checkInputSpecs(Configuration conf) {
    for (VertexInputFormat vertexInputFormat : vertexInputFormats) {
      vertexInputFormat.checkInputSpecs(conf);
    }
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    vertexInputFormats =
        VertexInputFormatDescription.createVertexInputFormats(getConf());
    if (vertexInputFormats.isEmpty()) {
      throw new IllegalStateException("setConf: Using MultiVertexInputFormat " +
          "without specifying vertex inputs");
    }
  }

  @Override
  public VertexReader<I, V, E> createVertexReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException {
    if (inputSplit instanceof InputSplitWithInputFormatIndex) {
      // When multithreaded input is used we need to make sure other threads
      // don't change context's configuration while we use it
      synchronized (context) {
        InputSplitWithInputFormatIndex split =
            (InputSplitWithInputFormatIndex) inputSplit;
        VertexInputFormat<I, V, E> vertexInputFormat =
            vertexInputFormats.get(split.getInputFormatIndex());
        VertexReader<I, V, E> vertexReader =
            vertexInputFormat.createVertexReader(split.getSplit(), context);
        return new WrappedVertexReader<I, V, E>(
            vertexReader, vertexInputFormat.getConf()) {
          @Override
          public void initialize(InputSplit inputSplit,
              TaskAttemptContext context) throws IOException,
              InterruptedException {
            // When multithreaded input is used we need to make sure other
            // threads don't change context's configuration while we use it
            synchronized (context) {
              super.initialize(inputSplit, context);
            }
          }
        };
      }
    } else {
      throw new IllegalStateException("createVertexReader: Got InputSplit " +
          "which was not created by this class: " +
          inputSplit.getClass().getName());
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context,
      int minSplitCountHint) throws IOException, InterruptedException {
    // When multithreaded input is used we need to make sure other threads don't
    // change context's configuration while we use it
    synchronized (context) {
      return MultiInputUtils.getSplits(
          context, minSplitCountHint, vertexInputFormats);
    }
  }

  @Override
  public void writeInputSplit(InputSplit inputSplit,
      DataOutput dataOutput) throws IOException {
    MultiInputUtils.writeInputSplit(inputSplit, dataOutput, vertexInputFormats);
  }

  @Override
  public InputSplit readInputSplit(
      DataInput dataInput) throws IOException, ClassNotFoundException {
    return MultiInputUtils.readInputSplit(dataInput, vertexInputFormats);
  }
}
