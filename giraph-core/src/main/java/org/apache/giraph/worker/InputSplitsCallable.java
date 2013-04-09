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

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MeterDesc;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.yammer.metrics.core.Meter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Abstract base class for loading vertex/edge input splits.
 * Every thread will has its own instance of WorkerClientRequestProcessor
 * to send requests.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class InputSplitsCallable<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements Callable<VertexEdgeCount> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(InputSplitsCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.get();
  /** Configuration */
  protected final ImmutableClassesGiraphConfiguration<I, V, E, M>
  configuration;
  /** Context */
  protected final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state */
  private final GraphState<I, V, E, M> graphState;
  /** Handles IPC communication */
  private final WorkerClientRequestProcessor<I, V, E, M>
  workerClientRequestProcessor;
  /**
   * Stores and processes the list of InputSplits advertised
   * in a tree of child znodes by the master.
   */
  private final InputSplitsHandler splitsHandler;
  /** ZooKeeperExt handle */
  private final ZooKeeperExt zooKeeperExt;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();
  /** Whether to prioritize local input splits. */
  private final boolean useLocality;

  // CHECKSTYLE: stop ParameterNumberCheck
  /**
   * Constructor.
   *
   * @param context Context
   * @param graphState Graph state
   * @param configuration Configuration
   * @param bspServiceWorker service worker
   * @param splitsHandler Handler for input splits
   * @param zooKeeperExt Handle to ZooKeeperExt
   */
  public InputSplitsCallable(
      Mapper<?, ?, ?, ?>.Context context,
      GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      InputSplitsHandler splitsHandler,
      ZooKeeperExt zooKeeperExt) {
    this.zooKeeperExt = zooKeeperExt;
    this.context = context;
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(
            context, configuration, bspServiceWorker);
    this.graphState = new GraphState<I, V, E, M>(graphState.getSuperstep(),
        graphState.getTotalNumVertices(), graphState.getTotalNumEdges(),
        context, graphState.getGraphTaskManager(), workerClientRequestProcessor,
        null);
    this.useLocality = configuration.useInputSplitLocality();
    this.splitsHandler = splitsHandler;
    this.configuration = configuration;
  }
  // CHECKSTYLE: resume ParameterNumberCheck

  /**
   * Get Meter tracking edges loaded
   *
   * @return Meter tracking edges loaded
   */
  public static Meter getTotalEdgesLoadedMeter() {
    return GiraphMetrics.get().perJobRequired()
        .getMeter(MeterDesc.EDGES_LOADED);
  }

  /**
   * Get Meter tracking number of vertices loaded.
   *
   * @return Meter for vertices loaded
   */
  public static Meter getTotalVerticesLoadedMeter() {
    return GiraphMetrics.get().perJobRequired()
        .getMeter(MeterDesc.VERTICES_LOADED);
  }

  /**
   * Load vertices/edges from the given input split.
   *
   * @param inputSplit Input split to load
   * @param graphState Graph state
   * @return Count of vertices and edges loaded
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract VertexEdgeCount readInputSplit(
      InputSplit inputSplit,
      GraphState<I, V, E, M> graphState)
    throws IOException, InterruptedException;

  @Override
  public VertexEdgeCount call() {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
    String inputSplitPath;
    int inputSplitsProcessed = 0;
    try {
      while ((inputSplitPath = splitsHandler.reserveInputSplit()) != null) {
        vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(
            loadInputSplit(inputSplitPath,
                graphState));
        context.progress();
        ++inputSplitsProcessed;
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("call: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("call: InterruptedException", e);
    } catch (IOException e) {
      throw new IllegalStateException("call: IOException", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("call: ClassNotFoundException", e);
    } catch (InstantiationException e) {
      throw new IllegalStateException("call: InstantiationException", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("call: IllegalAccessException", e);
    }

    if (LOG.isInfoEnabled()) {
      float seconds = Times.getNanosSince(TIME, startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      float verticesPerSecond = vertexEdgeCount.getVertexCount() / seconds;
      float edgesPerSecond = vertexEdgeCount.getEdgeCount() / seconds;
      LOG.info("call: Loaded " + inputSplitsProcessed + " " +
          "input splits in " + seconds + " secs, " + vertexEdgeCount +
          " " + verticesPerSecond + " vertices/sec, " +
          edgesPerSecond + " edges/sec");
    }
    try {
      workerClientRequestProcessor.flush();
    } catch (IOException e) {
      throw new IllegalStateException("call: Flushing failed.", e);
    }
    return vertexEdgeCount;
  }

  /**
   * Extract vertices from input split, saving them into a mini cache of
   * partitions.  Periodically flush the cache of vertices when a limit is
   * reached in readVerticeFromInputSplit.
   * Mark the input split finished when done.
   *
   * @param inputSplitPath ZK location of input split
   * @param graphState Current graph state
   * @return Mapping of vertex indices and statistics, or null if no data read
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private VertexEdgeCount loadInputSplit(
      String inputSplitPath,
      GraphState<I, V, E, M> graphState)
    throws IOException, ClassNotFoundException, InterruptedException,
      InstantiationException, IllegalAccessException {
    InputSplit inputSplit = getInputSplit(inputSplitPath);
    VertexEdgeCount vertexEdgeCount =
        readInputSplit(inputSplit, graphState);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadFromInputSplit: Finished loading " +
          inputSplitPath + " " + vertexEdgeCount);
    }
    splitsHandler.markInputSplitPathFinished(inputSplitPath);
    return vertexEdgeCount;
  }

  /**
   * Talk to ZooKeeper to convert the input split path to the actual
   * InputSplit.
   *
   * @param inputSplitPath Location in ZK of input split
   * @return instance of InputSplit
   * @throws IOException
   * @throws ClassNotFoundException
   */
  protected InputSplit getInputSplit(String inputSplitPath)
    throws IOException, ClassNotFoundException {
    byte[] splitList;
    try {
      splitList = zooKeeperExt.getData(inputSplitPath, false, null);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getInputSplit: KeeperException on " + inputSplitPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getInputSplit: IllegalStateException on " + inputSplitPath, e);
    }
    context.progress();

    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(splitList));
    if (useLocality) {
      Text.readString(inputStream); // location data unused here, skip
    }
    String inputSplitClass = Text.readString(inputStream);
    InputSplit inputSplit = (InputSplit)
        ReflectionUtils.newInstance(
            configuration.getClassByName(inputSplitClass),
            configuration);
    ((Writable) inputSplit).readFields(inputStream);

    if (LOG.isInfoEnabled()) {
      LOG.info("getInputSplit: Reserved " + inputSplitPath +
          " from ZooKeeper and got input split '" +
          inputSplit.toString() + "'");
    }
    return inputSplit;
  }
}
