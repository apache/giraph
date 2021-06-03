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
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.io.internal.WrappedMappingReader;
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
 * Mapping input format which wraps several mapping input formats.
 * Provides the way to read data from multiple sources,
 * using several different input formats.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge data
 * @param <B> Mapping target
 */
public class MultiMappingInputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  extends MappingInputFormat<I, V, E, B> {

  /** Mapping input formats */
  private List<MappingInputFormat<I, V, E, B>> mappingInputFormats;

  /**
   * Default constructor.
   */
  public MultiMappingInputFormat() {
  }

  @Override
  public void setConf(
    ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    mappingInputFormats =
      MappingInputFormatDescription.createMappingInputFormats(getConf());
    if (mappingInputFormats.isEmpty()) {
      throw new IllegalStateException("setConf: Using MultiVertexInputFormat " +
                                        "without specifying vertex inputs");
    }
  }

  @Override
  public MappingReader createMappingReader(
    InputSplit inputSplit, TaskAttemptContext context
  ) throws IOException {
    if (inputSplit instanceof InputSplitWithInputFormatIndex) {
      // When multithreaded input is used we need to make sure other threads
      // don't change context's configuration while we use it
      synchronized (context) {
        InputSplitWithInputFormatIndex split =
          (InputSplitWithInputFormatIndex) inputSplit;
        MappingInputFormat<I, V, E, B> mappingInputFormat =
          mappingInputFormats.get(split.getInputFormatIndex());
        MappingReader<I, V, E, B> mappingReader =
          mappingInputFormat.createMappingReader(split.getSplit(), context);
        return new WrappedMappingReader<I, V, E, B>(
          mappingReader, mappingInputFormat.getConf()) {
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
  public void checkInputSpecs(Configuration conf) {
    for (MappingInputFormat mappingInputFormat : mappingInputFormats) {
      mappingInputFormat.checkInputSpecs(conf);
    }
  }

  @Override
  public List<InputSplit> getSplits(
    JobContext context, int minSplitCountHint
  ) throws IOException, InterruptedException {
    synchronized (context) {
      return MultiInputUtils.getSplits(
        context, minSplitCountHint, mappingInputFormats);
    }
  }

  @Override
  public void writeInputSplit(InputSplit inputSplit, DataOutput dataOutput)
    throws IOException {
    MultiInputUtils.writeInputSplit(
      inputSplit, dataOutput, mappingInputFormats);
  }

  @Override
  public InputSplit readInputSplit(
    DataInput dataInput) throws IOException, ClassNotFoundException {
    return MultiInputUtils.readInputSplit(dataInput, mappingInputFormats);
  }
}
