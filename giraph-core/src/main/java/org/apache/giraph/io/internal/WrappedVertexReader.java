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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.job.HadoopUtils;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * For internal use only.
 *
 * Wraps {@link VertexReader} to make sure proper configuration
 * parameters are passed around, that parameters set in original
 * configuration are available in methods of this reader
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class WrappedVertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable> extends VertexReader<I, V, E> {
  /** VertexReader to delegate the methods to */
  private final VertexReader<I, V, E> baseVertexReader;

  /**
   * Constructor
   *
   * @param baseVertexReader VertexReader to delegate all the methods to
   * @param conf Configuration
   */
  public WrappedVertexReader(VertexReader<I, V, E> baseVertexReader,
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    this.baseVertexReader = baseVertexReader;
    super.setConf(conf);
    baseVertexReader.setConf(conf);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    // We don't want to use external configuration
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    baseVertexReader.initialize(inputSplit,
        HadoopUtils.makeTaskAttemptContext(getConf(), context));
  }

  @Override
  public void setWorkerGlobalCommUsage(WorkerGlobalCommUsage usage) {
    super.setWorkerGlobalCommUsage(usage);
    // Set aggregator usage for vertex reader
    baseVertexReader.setWorkerGlobalCommUsage(usage);
  }

  @Override
  public boolean nextVertex() throws IOException, InterruptedException {
    return baseVertexReader.nextVertex();
  }

  @Override
  public Vertex<I, V, E> getCurrentVertex() throws IOException,
      InterruptedException {
    return baseVertexReader.getCurrentVertex();
  }

  @Override
  public void close() throws IOException {
    baseVertexReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return baseVertexReader.getProgress();
  }
}
