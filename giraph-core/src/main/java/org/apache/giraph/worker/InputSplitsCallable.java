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
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphMetricsRegistry;
import org.apache.giraph.metrics.MeterDesc;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.util.PercentGauge;

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
 */
public abstract class InputSplitsCallable<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements Callable<VertexEdgeCount> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(InputSplitsCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.get();
  /** Configuration */
  protected final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Context */
  protected final Mapper<?, ?, ?, ?>.Context context;
  /** Handles IPC communication */
  protected final WorkerClientRequestProcessor<I, V, E>
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
   * @param configuration Configuration
   * @param bspServiceWorker service worker
   * @param splitsHandler Handler for input splits
   * @param zooKeeperExt Handle to ZooKeeperExt
   */
  public InputSplitsCallable(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      BspServiceWorker<I, V, E> bspServiceWorker,
      InputSplitsHandler splitsHandler,
      ZooKeeperExt zooKeeperExt) {
    this.zooKeeperExt = zooKeeperExt;
    this.context = context;
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E>(
            context, configuration, bspServiceWorker);
    this.useLocality = configuration.useInputSplitLocality();
    this.splitsHandler = splitsHandler;
    this.configuration = configuration;
  }
  // CHECKSTYLE: resume ParameterNumberCheck

  /**
   * Get input format
   *
   * @return Input format
   */
  public abstract GiraphInputFormat getInputFormat();

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
   * Get Counter tracking edges filtered
   *
   * @return Counter tracking edges filtered
   */
  public static Counter getTotalEdgesFilteredCounter() {
    return GiraphMetrics.get().perJobRequired()
        .getCounter(MetricNames.EDGES_FILTERED);
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
   * Get Counter tracking vertices filtered
   *
   * @return Counter tracking vertices filtered
   */
  public static Counter getTotalVerticesFilteredCounter() {
    return GiraphMetrics.get().perJobRequired()
        .getCounter(MetricNames.VERTICES_FILTERED);
  }

  /**
   * Initialize metrics used by this class and its subclasses.
   */
  public static void initMetrics() {
    GiraphMetricsRegistry metrics = GiraphMetrics.get().perJobRequired();

    final Counter edgesFiltered = getTotalEdgesFilteredCounter();
    final Meter edgesLoaded = getTotalEdgesLoadedMeter();

    metrics.getGauge(MetricNames.EDGES_FILTERED_PCT, new PercentGauge() {
      @Override protected double getNumerator() {
        return edgesFiltered.count();
      }

      @Override protected double getDenominator() {
        return edgesLoaded.count();
      }
    });

    final Counter verticesFiltered = getTotalVerticesFilteredCounter();
    final Meter verticesLoaded = getTotalVerticesLoadedMeter();

    metrics.getGauge(MetricNames.VERTICES_FILTERED_PCT, new PercentGauge() {
      @Override protected double getNumerator() {
        return verticesFiltered.count();
      }

      @Override protected double getDenominator() {
        return verticesLoaded.count();
      }
    });
  }

  /**
   * Load vertices/edges from the given input split.
   *
   * @param inputSplit Input split to load
   * @return Count of vertices and edges loaded
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract VertexEdgeCount readInputSplit(InputSplit inputSplit)
    throws IOException, InterruptedException;

  @Override
  public VertexEdgeCount call() {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
    String inputSplitPath;
    int inputSplitsProcessed = 0;
    try {
      while ((inputSplitPath = splitsHandler.reserveInputSplit()) != null) {
        vertexEdgeCount =
            vertexEdgeCount.incrVertexEdgeCount(loadInputSplit(inputSplitPath));
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
   * @return Mapping of vertex indices and statistics, or null if no data read
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private VertexEdgeCount loadInputSplit(
      String inputSplitPath)
    throws IOException, ClassNotFoundException, InterruptedException,
      InstantiationException, IllegalAccessException {
    InputSplit inputSplit = getInputSplit(inputSplitPath);
    VertexEdgeCount vertexEdgeCount = readInputSplit(inputSplit);
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
    InputSplit inputSplit = getInputFormat().readInputSplit(inputStream);

    if (LOG.isInfoEnabled()) {
      LOG.info("getInputSplit: Reserved " + inputSplitPath +
          " from ZooKeeper and got input split '" +
          inputSplit.toString() + "'");
    }
    return inputSplit;
  }
}
