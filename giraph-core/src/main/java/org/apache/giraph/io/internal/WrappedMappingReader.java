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
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.job.HadoopUtils;
import org.apache.giraph.mapping.MappingEntry;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * For internal use only.
 *
 * Wraps {@link org.apache.giraph.io.MappingReader} to make sure proper
 * configuration parameters are passed around, that parameters set in original
 * configuration are available in methods of this reader
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public class WrappedMappingReader<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  extends MappingReader<I, V, E, B> {
  /** User set baseMappingReader wrapped over */
  private final MappingReader<I, V, E, B> baseMappingReader;

  /**
   * Constructor
   *
   * @param baseMappingReader User set baseMappingReader
   * @param conf configuration
   */
  public WrappedMappingReader(MappingReader<I, V, E, B> baseMappingReader,
    ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    this.baseMappingReader = baseMappingReader;
    super.setConf(conf);
    baseMappingReader.setConf(conf);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    // We don't want to use external configuration
  }

  @Override
  public void initialize(InputSplit inputSplit,
    TaskAttemptContext context) throws IOException, InterruptedException {
    baseMappingReader.initialize(inputSplit,
        HadoopUtils.makeTaskAttemptContext(getConf(), context));
  }

  @Override
  public void setWorkerGlobalCommUsage(WorkerGlobalCommUsage usage) {
    super.setWorkerGlobalCommUsage(usage);
    // Set global communication usage for edge reader
    baseMappingReader.setWorkerGlobalCommUsage(usage);
  }

  @Override
  public boolean nextEntry() throws IOException, InterruptedException {
    return baseMappingReader.nextEntry();
  }

  @Override
  public MappingEntry<I, B> getCurrentEntry()
    throws IOException, InterruptedException {
    return baseMappingReader.getCurrentEntry();
  }


  @Override
  public void close() throws IOException {
    baseMappingReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return baseMappingReader.getProgress();
  }
}
