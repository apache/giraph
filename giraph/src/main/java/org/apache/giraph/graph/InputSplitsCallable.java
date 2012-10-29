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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.SystemTime;
import org.apache.giraph.utils.Time;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * Load as many input splits as possible.
 * Every thread will has its own instance of WorkerClientRequestProcessor
 * to send requests.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class InputSplitsCallable<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements Callable<VertexEdgeCount> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InputSplitsCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.getInstance();
  /** Context */
  private final Mapper<?, ?, ?, ?>.Context context;
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
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M>
  configuration;
  /** Total vertices loaded */
  private long totalVerticesLoaded = 0;
  /** Total edges loaded */
  private long totalEdgesLoaded = 0;
  /** Input split max vertices (-1 denotes all) */
  private final long inputSplitMaxVertices;
  /** Bsp service worker (only use thread-safe methods) */
  private final BspServiceWorker<I, V, E, M> bspServiceWorker;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();

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
   */
  public InputSplitsCallable(
      Mapper<?, ?, ?, ?>.Context context, GraphState<I, V, E, M> graphState,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      BspServiceWorker<I, V, E, M> bspServiceWorker,
      List<String> inputSplitPathList,
      WorkerInfo workerInfo,
      ZooKeeperExt zooKeeperExt)  {
    this.zooKeeperExt = zooKeeperExt;
    this.context = context;
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(
            context, configuration, bspServiceWorker);
    this.graphState = new GraphState<I, V, E, M>(graphState.getSuperstep(),
        graphState.getTotalNumVertices(), graphState.getTotalNumEdges(),
        context, graphState.getGraphMapper(), workerClientRequestProcessor);
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
    inputSplitMaxVertices = configuration.getInputSplitMaxVertices();
    this.bspServiceWorker = bspServiceWorker;
  }

  @Override
  public VertexEdgeCount call() {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
    String inputSplitPath = null;
    int inputSplitsProcessed = 0;
    try {
      while ((inputSplitPath = reserveInputSplit()) != null) {
        vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(
            loadVerticesFromInputSplit(inputSplitPath,
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
      float seconds = TIME.getNanosecondsSince(startNanos) /
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
    Stat reservedStat = null;
    while (true) {
      int reservedInputSplits = 0;
      for (String nextSplitToClaim : splitOrganizer) {
        context.progress();
        String tmpInputSplitReservedPath =
            nextSplitToClaim + BspServiceWorker.INPUT_SPLIT_RESERVED_NODE;
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
      bspServiceWorker.getInputSplitsStateChangedEvent().waitMsecs(
          60 * 1000);
      bspServiceWorker.getInputSplitsStateChangedEvent().reset();
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
        inputSplitPath + BspServiceWorker.INPUT_SPLIT_FINISHED_NODE;
    try {
      zooKeeperExt.createExt(inputSplitFinishedPath,
          null,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("loadVertices: " + inputSplitFinishedPath +
          " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "loadVertices: KeeperException on " +
              inputSplitFinishedPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "loadVertices: InterruptedException on " +
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
  private VertexEdgeCount loadVerticesFromInputSplit(
      String inputSplitPath,
      GraphState<I, V, E, M> graphState)
    throws IOException, ClassNotFoundException, InterruptedException,
      InstantiationException, IllegalAccessException {
    InputSplit inputSplit = getInputSplitForVertices(inputSplitPath);
    VertexEdgeCount vertexEdgeCount =
        readVerticesFromInputSplit(inputSplit, graphState);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadVerticesFromInputSplit: Finished loading " +
          inputSplitPath + " " + vertexEdgeCount);
    }
    markInputSplitPathFinished(inputSplitPath);
    return vertexEdgeCount;
  }

  /**
   * Talk to ZooKeeper to convert the input split path to the actual
   * InputSplit containing the vertices to read.
   *
   * @param inputSplitPath Location in ZK of input split
   * @return instance of InputSplit containing vertices to read
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private InputSplit getInputSplitForVertices(String inputSplitPath)
    throws IOException, ClassNotFoundException {
    byte[] splitList;
    try {
      splitList = zooKeeperExt.getData(inputSplitPath, false, null);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "loadVertices: KeeperException on " + inputSplitPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "loadVertices: IllegalStateException on " + inputSplitPath, e);
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
      LOG.info("getInputSplitForVertices: Reserved " + inputSplitPath +
          " from ZooKeeper and got input split '" +
          inputSplit.toString() + "'");
    }
    return inputSplit;
  }

  /**
   * Read vertices from input split.  If testing, the user may request a
   * maximum number of vertices to be read from an input split.
   *
   * @param inputSplit Input split to process with vertex reader
   * @param graphState Current graph state
   * @return Vertices and edges loaded from this input split
   * @throws IOException
   * @throws InterruptedException
   */
  private VertexEdgeCount readVerticesFromInputSplit(
      InputSplit inputSplit,
      GraphState<I, V, E, M> graphState)
    throws IOException, InterruptedException {
    VertexInputFormat<I, V, E, M> vertexInputFormat =
        configuration.createVertexInputFormat();
    VertexReader<I, V, E, M> vertexReader =
        vertexInputFormat.createVertexReader(inputSplit, context);
    vertexReader.initialize(inputSplit, context);
    long inputSplitVerticesLoaded = 0;
    long inputSplitEdgesLoaded = 0;
    while (vertexReader.nextVertex()) {
      Vertex<I, V, E, M> readerVertex =
          vertexReader.getCurrentVertex();
      if (readerVertex.getId() == null) {
        throw new IllegalArgumentException(
            "readVerticesFromInputSplit: Vertex reader returned a vertex " +
                "without an id!  - " + readerVertex);
      }
      if (readerVertex.getValue() == null) {
        readerVertex.setValue(configuration.createVertexValue());
      }
      readerVertex.setConf(configuration);
      readerVertex.setGraphState(graphState);

      PartitionOwner partitionOwner =
          bspServiceWorker.getVertexPartitionOwner(readerVertex.getId());
      graphState.getWorkerClientRequestProcessor().sendVertexRequest(
          partitionOwner, readerVertex);
      context.progress(); // do this before potential data transfer
      ++inputSplitVerticesLoaded;
      inputSplitEdgesLoaded += readerVertex.getNumEdges();

      // Update status every 250k vertices
      if (((inputSplitVerticesLoaded + totalVerticesLoaded) % 250000) == 0) {
        LoggerUtils.setStatusAndLog(context, LOG, Level.INFO,
            "readVerticesFromInputSplit: Loaded " +
                (inputSplitVerticesLoaded + totalVerticesLoaded) +
                " vertices " +
                (inputSplitEdgesLoaded + totalEdgesLoaded) + " edges " +
                MemoryUtils.getRuntimeMemoryStats());
      }

      // For sampling, or to limit outlier input splits, the number of
      // records per input split can be limited
      if (inputSplitMaxVertices > 0 &&
          inputSplitVerticesLoaded >= inputSplitMaxVertices) {
        if (LOG.isInfoEnabled()) {
          LOG.info("readVerticesFromInputSplit: Leaving the input " +
              "split early, reached maximum vertices " +
              inputSplitVerticesLoaded);
        }
        break;
      }
    }
    vertexReader.close();
    totalVerticesLoaded += inputSplitVerticesLoaded;
    totalEdgesLoaded += inputSplitEdgesLoaded;
    return new VertexEdgeCount(
        inputSplitVerticesLoaded, inputSplitEdgesLoaded);
  }
}

