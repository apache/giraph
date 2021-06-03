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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.utils.CallableFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Factory for {@link org.apache.giraph.worker.MappingInputSplitsCallable}s.
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public class MappingInputSplitsCallableFactory<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  implements CallableFactory<VertexEdgeCount> {
  /** Mapping input format */
  private final MappingInputFormat<I, V, E, B> mappingInputFormat;
  /** Mapper context. */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Configuration. */
  private final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** {@link BspServiceWorker} we're running on. */
  private final BspServiceWorker<I, V, E> bspServiceWorker;
  /** Handler for input splits */
  private final WorkerInputSplitsHandler splitsHandler;

  /**
   * Constructor.
   *
   * @param mappingInputFormat Mapping input format
   * @param context Mapper context
   * @param configuration Configuration
   * @param bspServiceWorker Calling {@link BspServiceWorker}
   * @param splitsHandler Splits handler
   */
  public MappingInputSplitsCallableFactory(
      MappingInputFormat<I, V, E, B> mappingInputFormat,
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      BspServiceWorker<I, V, E> bspServiceWorker,
      WorkerInputSplitsHandler splitsHandler) {
    this.mappingInputFormat = mappingInputFormat;
    this.context = context;
    this.configuration = configuration;
    this.bspServiceWorker = bspServiceWorker;
    this.splitsHandler = splitsHandler;
  }

  @Override
  public InputSplitsCallable<I, V, E> newCallable(int threadId) {
    return new MappingInputSplitsCallable<>(
        mappingInputFormat,
        context,
        configuration,
        bspServiceWorker,
        splitsHandler);
  }
}
