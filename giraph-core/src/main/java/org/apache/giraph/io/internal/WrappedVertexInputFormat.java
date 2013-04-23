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

package org.apache.giraph.io.internal;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * For internal use only.
 *
 * Wraps user set {@link VertexInputFormat} to make sure proper configuration
 * parameters are passed around, that user can set parameters in
 * configuration and they will be available in other methods related to this
 * format.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class WrappedVertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexInputFormat<I, V, E> {
  /** {@link VertexInputFormat} which is wrapped */
  private VertexInputFormat<I, V, E> originalInputFormat;

  /**
   * Constructor
   *
   * @param vertexInputFormat Vertex input format to wrap
   */
  public WrappedVertexInputFormat(
      VertexInputFormat<I, V, E> vertexInputFormat) {
    originalInputFormat = vertexInputFormat;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context,
      int minSplitCountHint) throws IOException, InterruptedException {
    getConf().updateConfiguration(context.getConfiguration());
    return originalInputFormat.getSplits(context, minSplitCountHint);
  }

  @Override
  public VertexReader<I, V, E> createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    getConf().updateConfiguration(context.getConfiguration());
    final VertexReader<I, V, E> vertexReader =
        originalInputFormat.createVertexReader(split, context);
    return new VertexReader<I, V, E>() {
      @Override
      public void setConf(
          ImmutableClassesGiraphConfiguration<I, V, E, Writable> conf) {
        super.setConf(conf);
        vertexReader.setConf(conf);
      }

      @Override
      public void initialize(InputSplit inputSplit,
          TaskAttemptContext context) throws IOException, InterruptedException {
        WrappedVertexInputFormat.this.getConf().updateConfiguration(
            context.getConfiguration());
        vertexReader.initialize(inputSplit, context);
      }

      @Override
      public boolean nextVertex() throws IOException, InterruptedException {
        return vertexReader.nextVertex();
      }

      @Override
      public Vertex<I, V, E, ?> getCurrentVertex() throws IOException,
          InterruptedException {
        return vertexReader.getCurrentVertex();
      }

      @Override
      public void close() throws IOException {
        vertexReader.close();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return vertexReader.getProgress();
      }
    };
  }
}
