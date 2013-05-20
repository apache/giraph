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
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
/*if_not[HADOOP_NON_COMMIT_JOB]*/
import org.apache.hadoop.mapreduce.JobStatus;
/*end[HADOOP_NON_COMMIT_JOB]*/
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * For internal use only.
 *
 * Wraps user set {@link VertexOutputFormat} to make sure proper configuration
 * parameters are passed around, that user can set parameters in
 * configuration and they will be available in other methods related to this
 * format.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class WrappedVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexOutputFormat<I, V, E> {
  /** {@link VertexOutputFormat} which is wrapped */
  private VertexOutputFormat<I, V, E> originalOutputFormat;

  /**
   * Constructor
   *
   * @param vertexOutputFormat Vertex output format to wrap
   */
  public WrappedVertexOutputFormat(
      VertexOutputFormat<I, V, E> vertexOutputFormat) {
    originalOutputFormat = vertexOutputFormat;
  }

  @Override
  public VertexWriter<I, V, E> createVertexWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    getConf().updateConfiguration(context.getConfiguration());
    final VertexWriter<I, V, E> vertexWriter =
        originalOutputFormat.createVertexWriter(context);
    return new VertexWriter<I, V, E>() {
      @Override
      public void setConf(
          ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        super.setConf(conf);
        vertexWriter.setConf(conf);
      }

      @Override
      public void initialize(
          TaskAttemptContext context) throws IOException, InterruptedException {
        getConf().updateConfiguration(context.getConfiguration());
        vertexWriter.initialize(context);
      }

      @Override
      public void close(
          TaskAttemptContext context) throws IOException, InterruptedException {
        getConf().updateConfiguration(context.getConfiguration());
        vertexWriter.close(context);
      }

      @Override
      public void writeVertex(
          Vertex<I, V, E> vertex) throws IOException, InterruptedException {
        vertexWriter.writeVertex(vertex);
      }
    };
  }

  @Override
  public void checkOutputSpecs(
      JobContext context) throws IOException, InterruptedException {
    getConf().updateConfiguration(context.getConfiguration());
    originalOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    getConf().updateConfiguration(context.getConfiguration());
    final OutputCommitter outputCommitter =
        originalOutputFormat.getOutputCommitter(context);
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.setupJob(context);
      }

      @Override
      public void setupTask(TaskAttemptContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.setupTask(context);
      }

      @Override
      public boolean needsTaskCommit(
          TaskAttemptContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        return outputCommitter.needsTaskCommit(context);
      }

      @Override
      public void commitTask(TaskAttemptContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.commitTask(context);
      }

      @Override
      public void abortTask(TaskAttemptContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.abortTask(context);
      }

      @Override
      public void cleanupJob(JobContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.cleanupJob(context);
      }

      /*if_not[HADOOP_NON_COMMIT_JOB]*/
      @Override
      public void commitJob(JobContext context) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.commitJob(context);
      }

      @Override
      public void abortJob(JobContext context,
          JobStatus.State state) throws IOException {
        getConf().updateConfiguration(context.getConfiguration());
        outputCommitter.abortJob(context, state);
      }
      /*end[HADOOP_NON_COMMIT_JOB]*/
    };
  }
}
