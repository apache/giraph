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

package org.apache.giraph.io.iterables;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.mapping.MappingEntry;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Wraps {@link GiraphReader} for mapping into
 * {@link org.apache.giraph.io.MappingReader}
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public class MappingReaderWrapper<I extends WritableComparable,
  V extends Writable, E extends  Writable, B extends Writable>
  extends MappingReader<I, V, E, B> {
  /** Wrapped mapping reader */
  private GiraphReader<MappingEntry<I, B>> mappingReader;
  /**
   * {@link org.apache.giraph.io.MappingReader}-like wrapper of
   * {@link #mappingReader}
   */
  private IteratorToReaderWrapper<MappingEntry<I, B>> iterator;

  /**
   * Constructor
   *
   * @param mappingReader user supplied mappingReader
   */
  public MappingReaderWrapper(GiraphReader<MappingEntry<I, B>> mappingReader) {
    this.mappingReader = mappingReader;
    iterator = new IteratorToReaderWrapper<>(mappingReader);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    conf.configureIfPossible(mappingReader);
  }

  @Override
  public boolean nextEntry() throws IOException, InterruptedException {
    return iterator.nextObject();
  }

  @Override
  public MappingEntry<I, B> getCurrentEntry()
    throws IOException, InterruptedException {
    return iterator.getCurrentObject();
  }


  @Override
  public void initialize(InputSplit inputSplit,
    TaskAttemptContext context) throws IOException, InterruptedException {
    mappingReader.initialize(inputSplit, context);
  }

  @Override
  public void close() throws IOException {
    mappingReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return mappingReader.getProgress();
  }
}
