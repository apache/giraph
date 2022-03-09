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

import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.job.HadoopUtils;
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
 * For internal use only.
 *
 * Wraps user set {@link org.apache.giraph.io.VertexInputFormat} to make
 * sure proper configuration parameters are passed around, that user can set
 * parameters in configuration and they will be available in other methods
 * related to this format.
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public class WrappedMappingInputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  extends MappingInputFormat<I, V, E, B> {
  /** originalInputFormat to wrap over */
  private MappingInputFormat<I, V, E, B> originalInputFormat;

  /**
   * Constructor
   *
   * @param mappingInputFormat original mappingInputFormat
   */
  public WrappedMappingInputFormat(
      MappingInputFormat<I, V, E, B> mappingInputFormat) {
    originalInputFormat = mappingInputFormat;
  }

  @Override
  public void checkInputSpecs(Configuration conf) {
    originalInputFormat.checkInputSpecs(conf);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    return originalInputFormat.getSplits(
        HadoopUtils.makeJobContext(getConf(), context),
        minSplitCountHint);
  }

  @Override
  public MappingReader<I, V, E, B> createMappingReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    final MappingReader<I, V, E, B> mappingReader = originalInputFormat
        .createMappingReader(split,
            HadoopUtils.makeTaskAttemptContext(getConf(), context));
    return new WrappedMappingReader<>(mappingReader, getConf());
  }


  @Override
  public void writeInputSplit(InputSplit inputSplit,
    DataOutput dataOutput) throws IOException {
    originalInputFormat.writeInputSplit(inputSplit, dataOutput);
  }

  @Override
  public InputSplit readInputSplit(
    DataInput dataInput) throws IOException, ClassNotFoundException {
    return originalInputFormat.readInputSplit(dataInput);
  }
}
