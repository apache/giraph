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
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

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
    implements CallableFactory<VertexEdgeCount> {
  /** Mapper context. */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state. */
  private final GraphState<I, V, E, M> graphState;
  /** Configuration. */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** {@link BspServiceWorker} we're running on. */
  private final BspServiceWorker<I, V, E, M> bspServiceWorker;
  /** Handler for input splits */
  private final InputSplitsHandler splitsHandler;
  /** {@link ZooKeeperExt} for this worker. */
  private final ZooKeeperExt zooKeeperExt;

  /**
   * Constructor.
   *
   * @param context Mapper context
   * @param graphState Graph state
   * @param configuration Configuration
   * @param bspServiceWorker Calling {@link BspServiceWorker}
   * @param splitsHandler Handler for input splits
   * @param zooKeeperExt {@link ZooKeeperExt} for this worker
   */
  public EdgeInputSplitsCallableFactory(
      Mapper<?, ?, ?, ?>.Context context,
      GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      InputSplitsHandler splitsHandler,
      ZooKeeperExt zooKeeperExt) {
    this.context = context;
    this.graphState = graphState;
    this.configuration = configuration;
    this.bspServiceWorker = bspServiceWorker;
    this.zooKeeperExt = zooKeeperExt;
    this.splitsHandler = splitsHandler;
  }

  @Override
  public InputSplitsCallable<I, V, E, M> newCallable(int threadId) {
    return new EdgeInputSplitsCallable<I, V, E, M>(
        context,
        graphState,
        configuration,
        bspServiceWorker,
        splitsHandler,
        zooKeeperExt);
  }
}
