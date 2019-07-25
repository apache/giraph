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

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.EdgeWriter;
import org.apache.giraph.job.HadoopUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * For internal use only.
 *
 * Wraps user set {@link EdgeOutputFormat} to make sure proper configuration
 * parameters are passed around, that user can set parameters in
 * configuration and they will be available in other methods related to this
 * format.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class WrappedEdgeOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends EdgeOutputFormat<I, V, E> {

  /** {@link EdgeOutputFormat} which is wrapped */
  private final EdgeOutputFormat<I, V, E> originalOutputFormat;

  /**
   * Constructor
   *
   * @param edgeOutputFormat Edge output format to wrap
   */
  public WrappedEdgeOutputFormat(
      EdgeOutputFormat<I, V, E> edgeOutputFormat) {
    originalOutputFormat = edgeOutputFormat;
  }

  @Override
  public EdgeWriter<I, V, E> createEdgeWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    final EdgeWriter<I, V, E> edgeWriter =
        originalOutputFormat.createEdgeWriter(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
    return new EdgeWriter<I, V, E>() {
      @Override
      public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        super.setConf(conf);
        edgeWriter.setConf(conf);
      }

      @Override
      public void initialize(TaskAttemptContext context)
        throws IOException, InterruptedException {
        edgeWriter.initialize(
          HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public void close(
          TaskAttemptContext context) throws IOException, InterruptedException {
        edgeWriter.close(
          HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public void writeEdge(I sourceId, V sourceValue, Edge<I, E> edge)
        throws IOException, InterruptedException {
        edgeWriter.writeEdge(sourceId, sourceValue, edge);
      }
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    originalOutputFormat.checkOutputSpecs(
        HadoopUtils.makeJobContext(getConf(), context));
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {

    final OutputCommitter outputCommitter =
        originalOutputFormat.getOutputCommitter(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));

    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext context) throws IOException {
        outputCommitter.setupJob(
            HadoopUtils.makeJobContext(getConf(), context));
      }

      @Override
      public void setupTask(TaskAttemptContext context) throws IOException {
        outputCommitter.setupTask(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public boolean needsTaskCommit(
          TaskAttemptContext context) throws IOException {
        return outputCommitter.needsTaskCommit(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public void commitTask(TaskAttemptContext context) throws IOException {
        outputCommitter.commitTask(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public void abortTask(TaskAttemptContext context) throws IOException {
        outputCommitter.abortTask(
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
      }

      @Override
      public void cleanupJob(JobContext context) throws IOException {
        outputCommitter.cleanupJob(
            HadoopUtils.makeJobContext(getConf(), context));
      }

      @Override
      public void commitJob(JobContext context) throws IOException {
        outputCommitter.commitJob(
            HadoopUtils.makeJobContext(getConf(), context));
      }

      @Override
      public void abortJob(JobContext context,
          JobStatus.State state) throws IOException {
        outputCommitter.abortJob(
            HadoopUtils.makeJobContext(getConf(), context), state);
      }
    };
  }

  @Override
  public void preWriting(TaskAttemptContext context) {
    originalOutputFormat.preWriting(context);
  }

  @Override
  public void postWriting(TaskAttemptContext context) {
    originalOutputFormat.postWriting(context);
  }
}
