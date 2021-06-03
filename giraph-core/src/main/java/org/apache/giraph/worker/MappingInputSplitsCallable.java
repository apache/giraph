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

package org.apache.giraph.worker;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.mapping.MappingEntry;
import org.apache.giraph.mapping.MappingStore;
import org.apache.giraph.io.InputType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Load as many mapping input splits as possible.
 * Every thread will has its own instance of WorkerClientRequestProcessor
 * to send requests.
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
@SuppressWarnings("unchecked")
public class MappingInputSplitsCallable<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  extends InputSplitsCallable<I, V, E> {
  /** User supplied mappingInputFormat */
  private final MappingInputFormat<I, V, E, B> mappingInputFormat;
  /** Link to bspServiceWorker */
  private final BspServiceWorker<I, V, E> bspServiceWorker;

  /**
   * Constructor
   *
   * @param mappingInputFormat mappingInputFormat
   * @param context Context
   * @param configuration Configuration
   * @param bspServiceWorker bsp service worker
   * @param splitsHandler Splits handler
   */
  public MappingInputSplitsCallable(
      MappingInputFormat<I, V, E, B> mappingInputFormat,
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      BspServiceWorker<I, V, E> bspServiceWorker,
      WorkerInputSplitsHandler splitsHandler) {
    super(context, configuration, bspServiceWorker, splitsHandler);
    this.mappingInputFormat = mappingInputFormat;
    this.bspServiceWorker = bspServiceWorker;
  }

  @Override
  public GiraphInputFormat getInputFormat() {
    return mappingInputFormat;
  }

  @Override
  public InputType getInputType() {
    return InputType.MAPPING;
  }

  @Override
  protected VertexEdgeCount readInputSplit(InputSplit inputSplit)
    throws IOException, InterruptedException {
    MappingReader<I, V, E, B> mappingReader =
        mappingInputFormat.createMappingReader(inputSplit, context);
    mappingReader.setConf(configuration);

    WorkerThreadGlobalCommUsage globalCommUsage = this.bspServiceWorker
        .getAggregatorHandler().newThreadAggregatorUsage();

    mappingReader.initialize(inputSplit, context);
    mappingReader.setWorkerGlobalCommUsage(globalCommUsage);

    int entriesLoaded = 0;
    MappingStore<I, B> mappingStore =
      (MappingStore<I, B>) bspServiceWorker.getLocalData().getMappingStore();

    while (mappingReader.nextEntry()) {
      MappingEntry<I, B> entry = mappingReader.getCurrentEntry();
      entriesLoaded += 1;
      mappingStore.addEntry(entry.getVertexId(), entry.getMappingTarget());
    }
    return new VertexEdgeCount(0, 0, entriesLoaded);
  }
}
