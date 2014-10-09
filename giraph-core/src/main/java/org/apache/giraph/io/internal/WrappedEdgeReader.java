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
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.job.HadoopUtils;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * For internal use only.
 *
 * Wraps {@link EdgeReader} to make sure proper configuration
 * parameters are passed around, that parameters set in original
 * configuration are available in methods of this reader
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class WrappedEdgeReader<I extends WritableComparable,
    E extends Writable> extends EdgeReader<I, E> {
  /** EdgeReader to delegate the methods to */
  private final EdgeReader<I, E> baseEdgeReader;

  /**
   * Constructor
   *
   * @param baseEdgeReader EdgeReader to delegate all the methods to
   * @param conf Configuration
   */
  public WrappedEdgeReader(EdgeReader<I, E> baseEdgeReader,
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    this.baseEdgeReader = baseEdgeReader;
    super.setConf(conf);
    baseEdgeReader.setConf(conf);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    // We don't want to use external configuration
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    baseEdgeReader.initialize(inputSplit,
        HadoopUtils.makeTaskAttemptContext(getConf(), context));
  }

  @Override
  public void setWorkerGlobalCommUsage(WorkerGlobalCommUsage usage) {
    super.setWorkerGlobalCommUsage(usage);
    // Set global communication usage for edge reader
    baseEdgeReader.setWorkerGlobalCommUsage(usage);
  }

  @Override
  public boolean nextEdge() throws IOException, InterruptedException {
    return baseEdgeReader.nextEdge();
  }

  @Override
  public I getCurrentSourceId() throws IOException, InterruptedException {
    return baseEdgeReader.getCurrentSourceId();
  }

  @Override
  public Edge<I, E> getCurrentEdge() throws IOException, InterruptedException {
    return baseEdgeReader.getCurrentEdge();
  }

  @Override
  public void close() throws IOException {
    baseEdgeReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return baseEdgeReader.getProgress();
  }
}
