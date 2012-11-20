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

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.utils.SystemTime;
import org.apache.giraph.utils.Time;
import org.apache.giraph.utils.Times;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
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
  /** Name of counter for vertices loaded */
  public static final String COUNTER_VERTICES_LOADED = "vertices-loaded";
  /** Name of counter for edges loaded */
  public static final String COUNTER_EDGES_LOADED = "edges-loaded";
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
  private final InputSplitPathOrganizer splitOrganizer;
  /** ZooKeeperExt handle */
  private final ZooKeeperExt zooKeeperExt;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();
  /** ZooKeeper input split reserved node. */
  private final String inputSplitReservedNode;
  /** ZooKeeper input split finished node. */
  private final String inputSplitFinishedNode;
  /** Input split events. */
  private final InputSplitEvents inputSplitEvents;

  // CHECKSTYLE: stop ParameterNumberCheck
  /**
   * Constructor.
   *
   * @param context Context
   * @param graphState Graph state
   * @param configuration Configuration
   * @param bspServiceWorker service worker
   * @param inputSplitPathList List of the paths of the input splits
   * @param workerInfo This worker's info
   * @param zooKeeperExt Handle to ZooKeeperExt
   * @param inputSplitReservedNode Path to input split reserved
   * @param inputSplitFinishedNode Path to input split finsished
   * @param inputSplitEvents Input split events
   */
  public InputSplitsCallable(
      Mapper<?, ?, ?, ?>.Context context,
      GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      List<String> inputSplitPathList,
      WorkerInfo workerInfo,
      ZooKeeperExt zooKeeperExt,
      String inputSplitReservedNode,
      String inputSplitFinishedNode,
      InputSplitEvents inputSplitEvents) {
    this.zooKeeperExt = zooKeeperExt;
    this.context = context;
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(
            context, configuration, bspServiceWorker);
    this.graphState = new GraphState<I, V, E, M>(graphState.getSuperstep(),
        graphState.getTotalNumVertices(), graphState.getTotalNumEdges(),
        context, graphState.getGraphMapper(), workerClientRequestProcessor,
        null);
    try {
      splitOrganizer = new InputSplitPathOrganizer(zooKeeperExt,
          inputSplitPathList, workerInfo.getHostname(), workerInfo.getPort());
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "InputSplitsCallable: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "InputSplitsCallable: InterruptedException", e);
    }
    this.configuration = configuration;
    this.inputSplitReservedNode = inputSplitReservedNode;
    this.inputSplitFinishedNode = inputSplitFinishedNode;
    this.inputSplitEvents = inputSplitEvents;
  }
  // CHECKSTYLE: resume ParameterNumberCheck

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
      while ((inputSplitPath = reserveInputSplit()) != null) {
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
   * Try to reserve an InputSplit for loading.  While InputSplits exists that
   * are not finished, wait until they are.
   *
   * NOTE: iterations on the InputSplit list only halt for each worker when it
   * has scanned the entire list once and found every split marked RESERVED.
   * When a worker fails, its Ephemeral RESERVED znodes will disappear,
   * allowing other iterating workers to claim it's previously read splits.
   * Only when the last worker left iterating on the list fails can a danger
   * of data loss occur. Since worker failure in INPUT_SUPERSTEP currently
   * causes job failure, this is OK. As the failure model evolves, this
   * behavior might need to change.
   *
   * @return reserved InputSplit or null if no unfinished InputSplits exist
   * @throws org.apache.zookeeper.KeeperException
   * @throws InterruptedException
   */
  private String reserveInputSplit()
    throws KeeperException, InterruptedException {
    String reservedInputSplitPath = null;
    Stat reservedStat;
    while (true) {
      int reservedInputSplits = 0;
      for (String nextSplitToClaim : splitOrganizer) {
        context.progress();
        String tmpInputSplitReservedPath = nextSplitToClaim +
            inputSplitReservedNode;
        reservedStat =
            zooKeeperExt.exists(tmpInputSplitReservedPath, true);
        if (reservedStat == null) {
          try {
            // Attempt to reserve this InputSplit
            zooKeeperExt.createExt(tmpInputSplitReservedPath,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                false);
            reservedInputSplitPath = nextSplitToClaim;
            if (LOG.isInfoEnabled()) {
              float percentFinished =
                  reservedInputSplits * 100.0f /
                      splitOrganizer.getPathListSize();
              LOG.info("reserveInputSplit: Reserved input " +
                  "split path " + reservedInputSplitPath +
                  ", overall roughly " +
                  + percentFinished +
                  "% input splits reserved");
            }
            return reservedInputSplitPath;
          } catch (KeeperException.NodeExistsException e) {
            LOG.info("reserveInputSplit: Couldn't reserve " +
                "(already reserved) inputSplit" +
                " at " + tmpInputSplitReservedPath);
          } catch (KeeperException e) {
            throw new IllegalStateException(
                "reserveInputSplit: KeeperException on reserve", e);
          } catch (InterruptedException e) {
            throw new IllegalStateException(
                "reserveInputSplit: InterruptedException " +
                    "on reserve", e);
          }
        } else {
          ++reservedInputSplits;
        }
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("reserveInputSplit: reservedPath = " +
            reservedInputSplitPath + ", " + reservedInputSplits +
            " of " + splitOrganizer.getPathListSize() +
            " InputSplits are finished.");
      }
      if (reservedInputSplits == splitOrganizer.getPathListSize()) {
        return null;
      }
      context.progress();
      // Wait for either a reservation to go away or a notification that
      // an InputSplit has finished.
      context.progress();
      inputSplitEvents.getStateChanged().waitMsecs(
          60 * 1000);
      inputSplitEvents.getStateChanged().reset();
    }
  }

  /**
   * Mark an input split path as completed by this worker.  This notifies
   * the master and the other workers that this input split has not only
   * been reserved, but also marked processed.
   *
   * @param inputSplitPath Path to the input split.
   */
  private void markInputSplitPathFinished(String inputSplitPath) {
    String inputSplitFinishedPath =
        inputSplitPath + inputSplitFinishedNode;
    try {
      zooKeeperExt.createExt(inputSplitFinishedPath,
          null,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("markInputSplitPathFinished: " + inputSplitFinishedPath +
          " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "markInputSplitPathFinished: KeeperException on " +
              inputSplitFinishedPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "markInputSplitPathFinished: InterruptedException on " +
              inputSplitFinishedPath, e);
    }
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
    markInputSplitPathFinished(inputSplitPath);
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
    Text.readString(inputStream); // location data unused here, skip
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
