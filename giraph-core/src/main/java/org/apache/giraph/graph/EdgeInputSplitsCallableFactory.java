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

package org.apache.giraph.graph;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.List;

/**
 * Factory for {@link EdgeInputSplitsCallable}s.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class EdgeInputSplitsCallableFactory<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements InputSplitsCallableFactory<I, V, E, M> {
  /** Mapper context. */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state. */
  private final GraphState<I, V, E, M> graphState;
  /** Configuration. */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** {@link BspServiceWorker} we're running on. */
  private final BspServiceWorker<I, V, E, M> bspServiceWorker;
  /** List of input split paths. */
  private final List<String> inputSplitPathList;
  /** Worker info. */
  private final WorkerInfo workerInfo;
  /** {@link ZooKeeperExt} for this worker. */
  private final ZooKeeperExt zooKeeperExt;

  /**
   * Constructor.
   *
   * @param context Mapper context
   * @param graphState Graph state
   * @param configuration Configuration
   * @param bspServiceWorker Calling {@link BspServiceWorker}
   * @param inputSplitPathList List of input split paths
   * @param workerInfo Worker info
   * @param zooKeeperExt {@link ZooKeeperExt} for this worker
   */
  public EdgeInputSplitsCallableFactory(
      Mapper<?, ?, ?, ?>.Context context,
      GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      List<String> inputSplitPathList,
      WorkerInfo workerInfo,
      ZooKeeperExt zooKeeperExt) {
    this.context = context;
    this.graphState = graphState;
    this.configuration = configuration;
    this.bspServiceWorker = bspServiceWorker;
    this.inputSplitPathList = inputSplitPathList;
    this.workerInfo = workerInfo;
    this.zooKeeperExt = zooKeeperExt;
  }

  @Override
  public InputSplitsCallable<I, V, E, M> newCallable() {
    return new EdgeInputSplitsCallable<I, V, E, M>(
        context,
        graphState,
        configuration,
        bspServiceWorker,
        inputSplitPathList,
        workerInfo,
        zooKeeperExt);
  }
}
