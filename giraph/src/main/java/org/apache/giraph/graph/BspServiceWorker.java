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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.netty.NettyWorkerClient;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerServer;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionExchange;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.PartitionStore;
import org.apache.giraph.graph.partition.WorkerGraphPartitioner;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import net.iharder.Base64;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceWorker}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class BspServiceWorker<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BspService<I, V, E, M>
    implements CentralizedServiceWorker<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);
  /** My process health znode */
  private String myHealthZnode;
  /** Worker info */
  private final WorkerInfo workerInfo;
  /** Worker graph partitioner */
  private final WorkerGraphPartitioner<I, V, E, M> workerGraphPartitioner;

  /** IPC Client */
  private final WorkerClient<I, V, E, M> workerClient;
  /** IPC Server */
  private final WorkerServer<I, V, E, M> workerServer;
  /** Master info */
  private WorkerInfo masterInfo = new WorkerInfo();
  /** Have the partition exchange children (workers) changed? */
  private final BspEvent partitionExchangeChildrenChanged;

  /** Worker Context */
  private final WorkerContext workerContext;
  /** Total vertices loaded */
  private long totalVerticesLoaded = 0;
  /** Total edges loaded */
  private long totalEdgesLoaded = 0;
  /** Input split max vertices (-1 denotes all) */
  private final long inputSplitMaxVertices;
  /**
   * Stores and processes the list of InputSplits advertised
   * in a tree of child znodes by the master.
   */
  private InputSplitPathOrganizer splitOrganizer = null;

  /** Handler for aggregators */
  private final WorkerAggregatorHandler aggregatorHandler;

  /**
   * Constructor for setting up the worker.
   *
   * @param serverPortList ZooKeeper server port list
   * @param sessionMsecTimeout Msecs to timeout connecting to ZooKeeper
   * @param context Mapper context
   * @param graphMapper Graph mapper
   * @throws IOException
   * @throws InterruptedException
   */
  public BspServiceWorker(
    String serverPortList,
    int sessionMsecTimeout,
    Mapper<?, ?, ?, ?>.Context context,
    GraphMapper<I, V, E, M> graphMapper)
    throws IOException, InterruptedException {
    super(serverPortList, sessionMsecTimeout, context, graphMapper);
    partitionExchangeChildrenChanged = new PredicateLock(context);
    registerBspEvent(partitionExchangeChildrenChanged);
    inputSplitMaxVertices = getConfiguration().getInputSplitMaxVertices();
    workerGraphPartitioner =
        getGraphPartitionerFactory().createWorkerGraphPartitioner();
    workerServer = new NettyWorkerServer<I, V, E, M>(getConfiguration(),
        this, context);
    workerClient = new NettyWorkerClient<I, V, E, M>(context,
        getConfiguration(), this);



    workerInfo = new WorkerInfo(
        getHostname(), getTaskPartition(), workerServer.getPort());
    this.workerContext =
        getConfiguration().createWorkerContext(null);

    aggregatorHandler = new WorkerAggregatorHandler();
  }

  @Override
  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public WorkerClient<I, V, E, M> getWorkerClient() {
    return workerClient;
  }

  /**
   * Intended to check the health of the node.  For instance, can it ssh,
   * dmesg, etc. For now, does nothing.
   * TODO: Make this check configurable by the user (i.e. search dmesg for
   * problems).
   *
   * @return True if healthy (always in this case).
   */
  public boolean isHealthy() {
    return true;
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
   * @throws KeeperException
   * @throws InterruptedException
   */
  private String reserveInputSplit()
    throws KeeperException, InterruptedException {
    if (null == splitOrganizer) {
      splitOrganizer = new InputSplitPathOrganizer(getZkExt(),
        inputSplitsPath, getHostname(), getWorkerInfo().getPort());
    }
    String reservedInputSplitPath = null;
    Stat reservedStat = null;
    final Mapper<?, ?, ?, ?>.Context context = getContext();
    while (true) {
      int reservedInputSplits = 0;
      for (String nextSplitToClaim : splitOrganizer) {
        context.progress();
        String tmpInputSplitReservedPath =
            nextSplitToClaim + INPUT_SPLIT_RESERVED_NODE;
        reservedStat =
            getZkExt().exists(tmpInputSplitReservedPath, true);
        if (reservedStat == null) {
          try {
            // Attempt to reserve this InputSplit
            getZkExt().createExt(tmpInputSplitReservedPath,
                null,
                Ids.OPEN_ACL_UNSAFE,
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
      getContext().progress();
      // Wait for either a reservation to go away or a notification that
      // an InputSplit has finished.
      context.progress();
      getInputSplitsStateChangedEvent().waitMsecs(60 * 1000);
      getInputSplitsStateChangedEvent().reset();
    }
  }

  /**
   * Load the vertices from the user-defined VertexReader into our partitions
   * of vertex ranges.  Do this until all the InputSplits have been processed.
   * All workers will try to do as many InputSplits as they can.  The master
   * will monitor progress and stop this once all the InputSplits have been
   * loaded and check-pointed.  Keep track of the last input split path to
   * ensure the input split cache is flushed prior to marking the last input
   * split complete.
   *
   * Use one or more threads to do the loading.
   *
   * @return Statistics of the vertices loaded
   * @throws IOException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private VertexEdgeCount loadVertices() throws IOException,
    ClassNotFoundException, InterruptedException, InstantiationException,
    IllegalAccessException, KeeperException {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();

    GraphState<I, V, E, M> graphState = new GraphState<I, V, E, M>(
        INPUT_SUPERSTEP, 0, 0, getContext(), getGraphMapper(),
        null);
    int numThreads = getConfiguration().getNumInputSplitsThreads();
    ExecutorService inputSplitsExecutor =
        Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat("load-%d").build());
    List<Future<VertexEdgeCount>> threadsFutures =
        Lists.newArrayListWithCapacity(numThreads);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadVertices: Using " + numThreads + " threads.");
    }
    for (int i = 0; i < numThreads; ++i) {
      Callable<VertexEdgeCount> inputSplitsCallable =
          new InputSplitsCallable<I, V, E, M>(
              getContext(),
              graphState,
              getConfiguration(),
              this,
              inputSplitsPath,
              getWorkerInfo(),
              getZkExt());
      threadsFutures.add(inputSplitsExecutor.submit(inputSplitsCallable));
    }

    // Wait until all the threads are done to wait on all requests
    for (Future<VertexEdgeCount> threadFuture : threadsFutures) {
      VertexEdgeCount threadVertexEdgeCount =
          ProgressableUtils.getFutureResult(threadFuture, getContext());
      vertexEdgeCount =
          vertexEdgeCount.incrVertexEdgeCount(threadVertexEdgeCount);
    }

    workerClient.waitAllRequests();
    inputSplitsExecutor.shutdown();
    return vertexEdgeCount;
  }

  @Override
  public WorkerInfo getMasterInfo() {
    return masterInfo;
  }

  @Override
  public FinishedSuperstepStats setup() {
    // Unless doing a restart, prepare for computation:
    // 1. Start superstep INPUT_SUPERSTEP (no computation)
    // 2. Wait until the INPUT_SPLIT_ALL_READY_PATH node has been created
    // 3. Process input splits until there are no more.
    // 4. Wait until the INPUT_SPLIT_ALL_DONE_PATH node has been created
    // 5. Wait for superstep INPUT_SUPERSTEP to complete.
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      setCachedSuperstep(getRestartedSuperstep());
      return new FinishedSuperstepStats(false, -1, -1);
    }

    JSONObject jobState = getJobState();
    if (jobState != null) {
      try {
        if ((ApplicationState.valueOf(jobState.getString(JSONOBJ_STATE_KEY)) ==
            ApplicationState.START_SUPERSTEP) &&
            jobState.getLong(JSONOBJ_SUPERSTEP_KEY) ==
            getSuperstep()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("setup: Restarting from an automated " +
                "checkpointed superstep " +
                getSuperstep() + ", attempt " +
                getApplicationAttempt());
          }
          setRestartedSuperstep(getSuperstep());
          return new FinishedSuperstepStats(false, -1, -1);
        }
      } catch (JSONException e) {
        throw new RuntimeException(
            "setup: Failed to get key-values from " +
                jobState.toString(), e);
      }
    }

    // Add the partitions for that this worker owns
    GraphState<I, V, E, M> graphState =
        new GraphState<I, V, E, M>(INPUT_SUPERSTEP, 0, 0,
            getContext(), getGraphMapper(), null);
    Collection<? extends PartitionOwner> masterSetPartitionOwners =
        startSuperstep(graphState);
    workerGraphPartitioner.updatePartitionOwners(
        getWorkerInfo(), masterSetPartitionOwners, getPartitionStore());

/*if[HADOOP_NON_SECURE]
    workerClient.setup();
else[HADOOP_NON_SECURE]*/
    workerClient.setup(getConfiguration().authenticate());
/*end[HADOOP_NON_SECURE]*/

    // Ensure the InputSplits are ready for processing before processing
    while (true) {
      Stat inputSplitsReadyStat;
      try {
        inputSplitsReadyStat =
            getZkExt().exists(inputSplitsAllReadyPath, true);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: KeeperException waiting on input splits", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: InterruptedException waiting on input splits", e);
      }
      if (inputSplitsReadyStat != null) {
        break;
      }
      getInputSplitsAllReadyEvent().waitForever();
      getInputSplitsAllReadyEvent().reset();
    }

    getContext().progress();

    try {
      VertexEdgeCount vertexEdgeCount = loadVertices();
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Finally loaded a total of " +
            vertexEdgeCount);
      }
    } catch (IOException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "IOException", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "ClassNotFoundException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "InterruptedException", e);
    } catch (InstantiationException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "InstantiationException", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "IllegalAccessException", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("setup: loadVertices failed due to " +
          "KeeperException", e);
    }
    getContext().progress();

    // Workers wait for each other to finish, coordinated by master
    String workerDonePath =
        inputSplitsDonePath + "/" + getWorkerInfo().getHostnameId();
    try {
      getZkExt().createExt(workerDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "setup: KeeperException creating worker done splits", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "setup: InterruptedException creating worker done splits", e);
    }
    while (true) {
      Stat inputSplitsDoneStat;
      try {
        inputSplitsDoneStat =
            getZkExt().exists(inputSplitsAllDonePath, true);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: KeeperException waiting on worker done splits", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: InterruptedException waiting on worker " +
                "done splits", e);
      }
      if (inputSplitsDoneStat != null) {
        break;
      }
      getInputSplitsAllDoneEvent().waitForever();
      getInputSplitsAllDoneEvent().reset();
    }

    // Create remaining partitions owned by this worker.
    for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
      if (partitionOwner.getWorkerInfo().equals(getWorkerInfo()) &&
          !getPartitionStore().hasPartition(
              partitionOwner.getPartitionId())) {
        Partition<I, V, E, M> partition =
            new Partition<I, V, E, M>(getConfiguration(),
                partitionOwner.getPartitionId(), getContext());
        getPartitionStore().addPartition(partition);
      }
    }

    // Generate the partition stats for the input superstep and process
    // if necessary
    List<PartitionStats> partitionStatsList =
        new ArrayList<PartitionStats>();
    for (Partition<I, V, E, M> partition :
        getPartitionStore().getPartitions()) {
      PartitionStats partitionStats =
          new PartitionStats(partition.getId(),
              partition.getVertices().size(),
              0,
              partition.getEdgeCount(),
              0);
      partitionStatsList.add(partitionStats);
    }
    workerGraphPartitioner.finalizePartitionStats(
        partitionStatsList, getPartitionStore());

    return finishSuperstep(graphState, partitionStatsList);
  }

  /**
   * Register the health of this worker for a given superstep
   *
   * @param superstep Superstep to register health on
   */
  private void registerHealth(long superstep) {
    JSONArray hostnamePort = new JSONArray();
    hostnamePort.put(getHostname());

    hostnamePort.put(workerInfo.getPort());

    String myHealthPath = null;
    if (isHealthy()) {
      myHealthPath = getWorkerInfoHealthyPath(getApplicationAttempt(),
          getSuperstep());
    } else {
      myHealthPath = getWorkerInfoUnhealthyPath(getApplicationAttempt(),
          getSuperstep());
    }
    myHealthPath = myHealthPath + "/" + workerInfo.getHostnameId();
    try {
      myHealthZnode = getZkExt().createExt(
          myHealthPath,
          WritableUtils.writeToByteArray(workerInfo),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("registerHealth: myHealthPath already exists (likely " +
          "from previous failure): " + myHealthPath +
          ".  Waiting for change in attempts " +
          "to re-join the application");
      getApplicationAttemptChangedEvent().waitForever();
      if (LOG.isInfoEnabled()) {
        LOG.info("registerHealth: Got application " +
            "attempt changed event, killing self");
      }
      throw new IllegalStateException(
          "registerHealth: Trying " +
              "to get the new application attempt by killing self", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + myHealthPath +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " + myHealthPath +
          " failed with InterruptedException", e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("registerHealth: Created my health node for attempt=" +
          getApplicationAttempt() + ", superstep=" +
          getSuperstep() + " with " + myHealthZnode +
          " and workerInfo= " + workerInfo);
    }
  }

  /**
   * Do this to help notify the master quicker that this worker has failed.
   */
  private void unregisterHealth() {
    LOG.error("unregisterHealth: Got failure, unregistering health on " +
        myHealthZnode + " on superstep " + getSuperstep());
    try {
      getZkExt().delete(myHealthZnode, -1);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "unregisterHealth: InterruptedException - Couldn't delete " +
              myHealthZnode, e);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "unregisterHealth: KeeperException - Couldn't delete " +
              myHealthZnode, e);
    }
  }

  @Override
  public void failureCleanup() {
    unregisterHealth();
  }

  @Override
  public Collection<? extends PartitionOwner> startSuperstep(
      GraphState<I, V, E, M> graphState) {
    // Algorithm:
    // 1. Communication service will combine message from previous
    //    superstep
    // 2. Register my health for the next superstep.
    // 3. Wait until the partition assignment is complete and get it
    // 4. Get the aggregator values from the previous superstep
    if (getSuperstep() != INPUT_SUPERSTEP) {
      workerServer.prepareSuperstep(graphState);
    }

    registerHealth(getSuperstep());

    String partitionAssignmentsNode =
        getPartitionAssignmentsPath(getApplicationAttempt(),
            getSuperstep());
    Collection<? extends PartitionOwner> masterSetPartitionOwners;
    try {
      while (getZkExt().exists(partitionAssignmentsNode, true) ==
          null) {
        getPartitionAssignmentsReadyChangedEvent().waitForever();
        getPartitionAssignmentsReadyChangedEvent().reset();
      }
      List<? extends Writable> writableList =
          WritableUtils.readListFieldsFromZnode(
              getZkExt(),
              partitionAssignmentsNode,
              false,
              null,
              workerGraphPartitioner.createPartitionOwner().getClass(),
              getConfiguration());

      @SuppressWarnings("unchecked")
      Collection<? extends PartitionOwner> castedWritableList =
        (Collection<? extends PartitionOwner>) writableList;
      masterSetPartitionOwners = castedWritableList;
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "startSuperstep: KeeperException getting assignments", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "startSuperstep: InterruptedException getting assignments", e);
    }

    // get address of master
    WritableUtils.readFieldsFromZnode(getZkExt(), currentMasterPath, false,
        null, masterInfo);

    if (LOG.isInfoEnabled()) {
      LOG.info("startSuperstep: Ready for computation on superstep " +
          getSuperstep() + " since worker " +
          "selection and vertex range assignments are done in " +
          partitionAssignmentsNode);
    }

    if (getSuperstep() != INPUT_SUPERSTEP) {
      aggregatorHandler.prepareSuperstep(getSuperstep(), this);
    }
    getContext().setStatus("startSuperstep: " +
        getGraphMapper().getMapFunctions().toString() +
        " - Attempt=" + getApplicationAttempt() +
        ", Superstep=" + getSuperstep());
    return masterSetPartitionOwners;
  }

  @Override
  public FinishedSuperstepStats finishSuperstep(
      GraphState<I, V, E, M> graphState,
      List<PartitionStats> partitionStatsList) {
    // This barrier blocks until success (or the master signals it to
    // restart).
    //
    // Master will coordinate the barriers and aggregate "doneness" of all
    // the vertices.  Each worker will:
    // 1. Ensure that the requests are complete
    // 2. Execute user postSuperstep() if necessary.
    // 3. Save aggregator values that are in use.
    // 4. Report the statistics (vertices, edges, messages, etc.)
    //    of this worker
    // 5. Let the master know it is finished.
    // 6. Wait for the master's global stats, and check if done
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Waiting on all requests, superstep " +
          getSuperstep() + " " +
          MemoryUtils.getRuntimeMemoryStats());
    }
    workerClient.waitAllRequests();

    long workerSentMessages = 0;
    for (PartitionStats partitionStats : partitionStatsList) {
      workerSentMessages += partitionStats.getMessagesSentCount();
    }

    if (getSuperstep() != INPUT_SUPERSTEP) {
      getWorkerContext().setGraphState(graphState);
      getWorkerContext().postSuperstep();
      getContext().progress();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Superstep " + getSuperstep() +
          ", messages = " + workerSentMessages + " " +
          MemoryUtils.getRuntimeMemoryStats());
    }

    byte[] aggregatorArray =
        aggregatorHandler.finishSuperstep(getSuperstep());
    Collection<PartitionStats> finalizedPartitionStats =
        workerGraphPartitioner.finalizePartitionStats(
            partitionStatsList, getPartitionStore());
    List<PartitionStats> finalizedPartitionStatsList =
        new ArrayList<PartitionStats>(finalizedPartitionStats);
    byte [] partitionStatsBytes =
        WritableUtils.writeListToByteArray(finalizedPartitionStatsList);
    JSONObject workerFinishedInfoObj = new JSONObject();
    try {
      workerFinishedInfoObj.put(JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY,
          Base64.encodeBytes(aggregatorArray));
      workerFinishedInfoObj.put(JSONOBJ_PARTITION_STATS_KEY,
          Base64.encodeBytes(partitionStatsBytes));
      workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGES_KEY,
          workerSentMessages);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep()) +
        "/" + getHostnamePartitionId();
    try {
      getZkExt().createExt(finishedWorkerPath,
          workerFinishedInfoObj.toString().getBytes(),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("finishSuperstep: finished worker path " +
          finishedWorkerPath + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with InterruptedException", e);
    }

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "finishSuperstep: (waiting for rest " +
            "of workers) " +
            getGraphMapper().getMapFunctions().toString() +
            " - Attempt=" + getApplicationAttempt() +
            ", Superstep=" + getSuperstep());

    String superstepFinishedNode =
        getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
    try {
      while (getZkExt().exists(superstepFinishedNode, true) == null) {
        getSuperstepFinishedEvent().waitForever();
        getSuperstepFinishedEvent().reset();
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal completion of superstep " + getSuperstep(), e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal completion of superstep " + getSuperstep(), e);
    }
    GlobalStats globalStats = new GlobalStats();
    WritableUtils.readFieldsFromZnode(
        getZkExt(), superstepFinishedNode, false, null, globalStats);
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
          " with global stats " + globalStats);
    }
    incrCachedSuperstep();
    getContext().setStatus("finishSuperstep: (all workers done) " +
        getGraphMapper().getMapFunctions().toString() +
        " - Attempt=" + getApplicationAttempt() +
        ", Superstep=" + getSuperstep());
    return new FinishedSuperstepStats(
        globalStats.getHaltComputation(),
        globalStats.getVertexCount(),
        globalStats.getEdgeCount());
  }

  /**
   * Save the vertices using the user-defined VertexOutputFormat from our
   * vertexArray based on the split.
   * @throws InterruptedException
   */
  private void saveVertices() throws IOException, InterruptedException {
    if (getConfiguration().getVertexOutputFormatClass() == null) {
      LOG.warn("saveVertices: " +
          GiraphConfiguration.VERTEX_OUTPUT_FORMAT_CLASS +
          " not specified -- there will be no saved output");
      return;
    }

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveVertices: Starting to save vertices");
    VertexOutputFormat<I, V, E> vertexOutputFormat =
        getConfiguration().createVertexOutputFormat();
    VertexWriter<I, V, E> vertexWriter =
        vertexOutputFormat.createVertexWriter(getContext());
    vertexWriter.initialize(getContext());
    for (Partition<I, V, E, M> partition :
        getPartitionStore().getPartitions()) {
      for (Vertex<I, V, E, M> vertex : partition.getVertices()) {
        getContext().progress();
        vertexWriter.writeVertex(vertex);
      }
      getContext().progress();
    }
    vertexWriter.close(getContext());
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveVertices: Done saving vertices");
  }

  @Override
  public void cleanup() throws IOException, InterruptedException {
    workerClient.closeConnections();
    setCachedSuperstep(getSuperstep() - 1);
    saveVertices();
    // All worker processes should denote they are done by adding special
    // znode.  Once the number of znodes equals the number of partitions
    // for workers and masters, the master will clean up the ZooKeeper
    // znodes associated with this job.
    String workerCleanedUpPath = cleanedUpPath  + "/" +
        getTaskPartition() + WORKER_SUFFIX;
    try {
      String finalFinishedPath =
          getZkExt().createExt(workerCleanedUpPath,
              null,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT,
              true);
      if (LOG.isInfoEnabled()) {
        LOG.info("cleanup: Notifying master its okay to cleanup with " +
            finalFinishedPath);
      }
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("cleanup: Couldn't create finished node '" +
            workerCleanedUpPath);
      }
    } catch (KeeperException e) {
      // Cleaning up, it's okay to fail after cleanup is successful
      LOG.error("cleanup: Got KeeperException on notification " +
          "to master about cleanup", e);
    } catch (InterruptedException e) {
      // Cleaning up, it's okay to fail after cleanup is successful
      LOG.error("cleanup: Got InterruptedException on notification " +
          "to master about cleanup", e);
    }
    try {
      getZkExt().close();
    } catch (InterruptedException e) {
      // cleanup phase -- just log the error
      LOG.error("cleanup: Zookeeper failed to close with " + e);
    }

    // Preferably would shut down the service only after
    // all clients have disconnected (or the exceptions on the
    // client side ignored).
    workerServer.close();
  }

  @Override
  public void storeCheckpoint() throws IOException {
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "storeCheckpoint: Starting checkpoint " +
            getGraphMapper().getMapFunctions().toString() +
            " - Attempt=" + getApplicationAttempt() +
            ", Superstep=" + getSuperstep());

    // Algorithm:
    // For each partition, dump vertices and messages
    Path metadataFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_METADATA_POSTFIX);
    Path verticesFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_VERTICES_POSTFIX);
    Path validFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_VALID_POSTFIX);

    // Remove these files if they already exist (shouldn't though, unless
    // of previous failure of this worker)
    if (getFs().delete(validFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed valid file " +
          validFilePath);
    }
    if (getFs().delete(metadataFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed metadata file " +
          metadataFilePath);
    }
    if (getFs().delete(verticesFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed file " + verticesFilePath);
    }

    FSDataOutputStream verticesOutputStream =
        getFs().create(verticesFilePath);
    ByteArrayOutputStream metadataByteStream = new ByteArrayOutputStream();
    DataOutput metadataOutput = new DataOutputStream(metadataByteStream);
    for (Partition<I, V, E, M> partition :
        getPartitionStore().getPartitions()) {
      long startPos = verticesOutputStream.getPos();
      partition.write(verticesOutputStream);
      // write messages
      getServerData().getCurrentMessageStore().writePartition(
          verticesOutputStream, partition.getId());
      // Write the metadata for this partition
      // Format:
      // <index count>
      //   <index 0 start pos><partition id>
      //   <index 1 start pos><partition id>
      metadataOutput.writeLong(startPos);
      metadataOutput.writeInt(partition.getId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("storeCheckpoint: Vertex file starting " +
            "offset = " + startPos + ", length = " +
            (verticesOutputStream.getPos() - startPos) +
            ", partition = " + partition.toString());
      }
      getContext().progress();
    }
    // Metadata is buffered and written at the end since it's small and
    // needs to know how many partitions this worker owns
    FSDataOutputStream metadataOutputStream =
        getFs().create(metadataFilePath);
    metadataOutputStream.writeInt(getPartitionStore().getNumPartitions());
    metadataOutputStream.write(metadataByteStream.toByteArray());
    metadataOutputStream.close();
    verticesOutputStream.close();
    if (LOG.isInfoEnabled()) {
      LOG.info("storeCheckpoint: Finished metadata (" +
          metadataFilePath + ") and vertices (" + verticesFilePath + ").");
    }

    getFs().createNewFile(validFilePath);

    // Notify master that checkpoint is stored
    String workerWroteCheckpoint =
        getWorkerWroteCheckpointPath(getApplicationAttempt(),
            getSuperstep()) + "/" + getHostnamePartitionId();
    try {
      getZkExt().createExt(workerWroteCheckpoint,
          new byte[0],
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("finishSuperstep: wrote checkpoint worker path " +
          workerWroteCheckpoint + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + workerWroteCheckpoint +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " +
          workerWroteCheckpoint +
          " failed with InterruptedException", e);
    }
  }

  @Override
  public VertexEdgeCount loadCheckpoint(long superstep) {
    try {
      // clear old message stores
      getServerData().getIncomingMessageStore().clearAll();
      getServerData().getCurrentMessageStore().clearAll();
    } catch (IOException e) {
      throw new RuntimeException(
          "loadCheckpoint: Failed to clear message stores ", e);
    }

    // Algorithm:
    // Examine all the partition owners and load the ones
    // that match my hostname and id from the master designated checkpoint
    // prefixes.
    long startPos = 0;
    int loadedPartitions = 0;
    for (PartitionOwner partitionOwner :
      workerGraphPartitioner.getPartitionOwners()) {
      if (partitionOwner.getWorkerInfo().equals(getWorkerInfo())) {
        String metadataFile =
            partitionOwner.getCheckpointFilesPrefix() +
            CHECKPOINT_METADATA_POSTFIX;
        String partitionsFile =
            partitionOwner.getCheckpointFilesPrefix() +
            CHECKPOINT_VERTICES_POSTFIX;
        try {
          int partitionId = -1;
          DataInputStream metadataStream =
              getFs().open(new Path(metadataFile));
          int partitions = metadataStream.readInt();
          for (int i = 0; i < partitions; ++i) {
            startPos = metadataStream.readLong();
            partitionId = metadataStream.readInt();
            if (partitionId == partitionOwner.getPartitionId()) {
              break;
            }
          }
          if (partitionId != partitionOwner.getPartitionId()) {
            throw new IllegalStateException(
                "loadCheckpoint: " + partitionOwner +
                " not found!");
          }
          metadataStream.close();
          Partition<I, V, E, M> partition =
              new Partition<I, V, E, M>(
                  getConfiguration(),
                  partitionId,
                  getContext());
          DataInputStream partitionsStream =
              getFs().open(new Path(partitionsFile));
          if (partitionsStream.skip(startPos) != startPos) {
            throw new IllegalStateException(
                "loadCheckpoint: Failed to skip " + startPos +
                " on " + partitionsFile);
          }
          partition.readFields(partitionsStream);
          if (partitionsStream.readBoolean()) {
            getServerData().getCurrentMessageStore().readFieldsForPartition(
                partitionsStream, partitionId);
          }
          partitionsStream.close();
          if (LOG.isInfoEnabled()) {
            LOG.info("loadCheckpoint: Loaded partition " +
                partition);
          }
          if (getPartitionStore().hasPartition(partitionId)) {
            throw new IllegalStateException(
                "loadCheckpoint: Already has partition owner " +
                    partitionOwner);
          }
          getPartitionStore().addPartition(partition);
          getContext().progress();
          ++loadedPartitions;
        } catch (IOException e) {
          throw new RuntimeException(
              "loadCheckpoint: Failed to get partition owner " +
                  partitionOwner, e);
        }
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("loadCheckpoint: Loaded " + loadedPartitions +
          " partitions of out " +
          workerGraphPartitioner.getPartitionOwners().size() +
          " total.");
    }

    // Load global statistics
    GlobalStats globalStats = null;
    String finalizedCheckpointPath =
        getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
    try {
      DataInputStream finalizedStream =
          getFs().open(new Path(finalizedCheckpointPath));
      globalStats = new GlobalStats();
      globalStats.readFields(finalizedStream);
    } catch (IOException e) {
      throw new IllegalStateException(
          "loadCheckpoint: Failed to load global statistics", e);
    }

    // Communication service needs to setup the connections prior to
    // processing vertices
/*if[HADOOP_NON_SECURE]
    workerClient.setup();
else[HADOOP_NON_SECURE]*/
    workerClient.setup(getConfiguration().authenticate());
/*end[HADOOP_NON_SECURE]*/
    return new VertexEdgeCount(globalStats.getVertexCount(),
        globalStats.getEdgeCount());
  }

  /**
   * Send the worker partitions to their destination workers
   *
   * @param workerPartitionMap Map of worker info to the partitions stored
   *        on this worker to be sent
   */
  private void sendWorkerPartitions(
      Map<WorkerInfo, List<Integer>> workerPartitionMap) {
    List<Entry<WorkerInfo, List<Integer>>> randomEntryList =
        new ArrayList<Entry<WorkerInfo, List<Integer>>>(
            workerPartitionMap.entrySet());
    Collections.shuffle(randomEntryList);
    WorkerClientRequestProcessor<I, V, E, M> workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(getContext(),
            getConfiguration(), this);
    for (Entry<WorkerInfo, List<Integer>> workerPartitionList :
      randomEntryList) {
      for (Integer partitionId : workerPartitionList.getValue()) {
        Partition<I, V, E, M> partition =
            getPartitionStore().removePartition(partitionId);
        if (partition == null) {
          throw new IllegalStateException(
              "sendWorkerPartitions: Couldn't find partition " +
                  partitionId + " to send to " +
                  workerPartitionList.getKey());
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("sendWorkerPartitions: Sending worker " +
              workerPartitionList.getKey() + " partition " +
              partitionId);
        }
        workerClientRequestProcessor.sendPartitionRequest(
            workerPartitionList.getKey(),
            partition);
      }
    }


    try {
      workerClientRequestProcessor.flush();
      workerClient.waitAllRequests();
    } catch (IOException e) {
      throw new IllegalStateException("sendWorkerPartitions: Flush failed", e);
    }
    String myPartitionExchangeDonePath =
        getPartitionExchangeWorkerPath(
            getApplicationAttempt(), getSuperstep(), getWorkerInfo());
    try {
      getZkExt().createExt(myPartitionExchangeDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "sendWorkerPartitions: KeeperException to create " +
              myPartitionExchangeDonePath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "sendWorkerPartitions: InterruptedException to create " +
              myPartitionExchangeDonePath, e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("sendWorkerPartitions: Done sending all my partitions.");
    }
  }

  @Override
  public final void exchangeVertexPartitions(
      Collection<? extends PartitionOwner> masterSetPartitionOwners) {
    // 1. Fix the addresses of the partition ids if they have changed.
    // 2. Send all the partitions to their destination workers in a random
    //    fashion.
    // 3. Notify completion with a ZooKeeper stamp
    // 4. Wait for all my dependencies to be done (if any)
    // 5. Add the partitions to myself.
    PartitionExchange partitionExchange =
        workerGraphPartitioner.updatePartitionOwners(
            getWorkerInfo(), masterSetPartitionOwners, getPartitionStore());
    workerClient.openConnections(getPartitionOwners());

    Map<WorkerInfo, List<Integer>> sendWorkerPartitionMap =
        partitionExchange.getSendWorkerPartitionMap();
    if (!getPartitionStore().isEmpty()) {
      sendWorkerPartitions(sendWorkerPartitionMap);
    }

    Set<WorkerInfo> myDependencyWorkerSet =
        partitionExchange.getMyDependencyWorkerSet();
    Set<String> workerIdSet = new HashSet<String>();
    for (WorkerInfo tmpWorkerInfo : myDependencyWorkerSet) {
      if (!workerIdSet.add(tmpWorkerInfo.getHostnameId())) {
        throw new IllegalStateException(
            "exchangeVertexPartitions: Duplicate entry " + tmpWorkerInfo);
      }
    }
    if (myDependencyWorkerSet.isEmpty() && getPartitionStore().isEmpty()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("exchangeVertexPartitions: Nothing to exchange, " +
            "exiting early");
      }
      return;
    }

    String vertexExchangePath =
        getPartitionExchangePath(getApplicationAttempt(), getSuperstep());
    List<String> workerDoneList;
    try {
      while (true) {
        workerDoneList = getZkExt().getChildrenExt(
            vertexExchangePath, true, false, false);
        workerIdSet.removeAll(workerDoneList);
        if (workerIdSet.isEmpty()) {
          break;
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("exchangeVertexPartitions: Waiting for workers " +
              workerIdSet);
        }
        getPartitionExchangeChildrenChangedEvent().waitForever();
        getPartitionExchangeChildrenChangedEvent().reset();
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("exchangeVertexPartitions: Done with exchange.");
    }
  }

  /**
   * Get event when the state of a partition exchange has changed.
   *
   * @return Event to check.
   */
  public final BspEvent getPartitionExchangeChildrenChangedEvent() {
    return partitionExchangeChildrenChanged;
  }

  @Override
  protected boolean processEvent(WatchedEvent event) {
    boolean foundEvent = false;
    if (event.getPath().startsWith(masterJobStatePath) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("processEvent: Job state changed, checking " +
            "to see if it needs to restart");
      }
      JSONObject jsonObj = getJobState();
      try {
        if ((ApplicationState.valueOf(jsonObj.getString(JSONOBJ_STATE_KEY)) ==
            ApplicationState.START_SUPERSTEP) &&
            jsonObj.getLong(JSONOBJ_APPLICATION_ATTEMPT_KEY) !=
            getApplicationAttempt()) {
          LOG.fatal("processEvent: Worker will restart " +
              "from command - " + jsonObj.toString());
          System.exit(-1);
        }
      } catch (JSONException e) {
        throw new RuntimeException(
            "processEvent: Couldn't properly get job state from " +
                jsonObj.toString());
      }
      foundEvent = true;
    } else if (event.getPath().contains(PARTITION_EXCHANGE_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("processEvent : partitionExchangeChildrenChanged " +
            "(at least one worker is done sending partitions)");
      }
      partitionExchangeChildrenChanged.signal();
      foundEvent = true;
    }

    return foundEvent;
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public PartitionStore<I, V, E, M> getPartitionStore() {
    return getServerData().getPartitionStore();
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return workerGraphPartitioner.getPartitionOwners();
  }

  @Override
  public PartitionOwner getVertexPartitionOwner(I vertexId) {
    return workerGraphPartitioner.getPartitionOwner(vertexId);
  }

  @Override
  public Partition<I, V, E, M> getPartition(I vertexId) {
    return getPartitionStore().getPartition(getPartitionId(vertexId));
  }

  @Override
  public Integer getPartitionId(I vertexId) {
    PartitionOwner partitionOwner = getVertexPartitionOwner(vertexId);
    return partitionOwner.getPartitionId();
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return getPartitionStore().hasPartition(partitionId);
  }

  @Override
  public Vertex<I, V, E, M> getVertex(I vertexId) {
    PartitionOwner partitionOwner = getVertexPartitionOwner(vertexId);
    if (getPartitionStore().hasPartition(partitionOwner.getPartitionId())) {
      return getPartitionStore().getPartition(
          partitionOwner.getPartitionId()).getVertex(vertexId);
    } else {
      return null;
    }
  }

  @Override
  public ServerData<I, V, E, M> getServerData() {
    return workerServer.getServerData();
  }

  @Override
  public WorkerAggregatorUsage getAggregatorUsage() {
    return aggregatorHandler;
  }
}
