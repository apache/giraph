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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import net.iharder.Base64;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.aggregators.WorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.queue.AsyncMessageStoreWrapper;
import org.apache.giraph.comm.netty.NettyWorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClient;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerServer;
import org.apache.giraph.comm.requests.PartitionStatsRequest;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AddressesAndPartitionsWritable;
import org.apache.giraph.graph.FinishedSuperstepStats;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.EdgeWriter;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.superstep_output.SuperstepOutput;
import org.apache.giraph.mapping.translate.TranslateEdge;
import org.apache.giraph.master.MasterInfo;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphTimer;
import org.apache.giraph.metrics.GiraphTimerContext;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.metrics.WorkerSuperstepMetrics;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionExchange;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.utils.BlockingElementsSet;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.giraph.utils.JMapHistoDumper;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.ReactiveJMapHistoDumper;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
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

import com.google.common.collect.Lists;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceWorker}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class BspServiceWorker<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends BspService<I, V, E>
    implements CentralizedServiceWorker<I, V, E>,
    ResetSuperstepMetricsObserver {
  /** Name of gauge for time spent waiting on other workers */
  public static final String TIMER_WAIT_REQUESTS = "wait-requests-us";
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);
  /** My process health znode */
  private String myHealthZnode;
  /** Worker info */
  private final WorkerInfo workerInfo;
  /** Worker graph partitioner */
  private final WorkerGraphPartitioner<I, V, E> workerGraphPartitioner;
  /** Local Data for each worker */
  private final LocalData<I, V, E, ? extends Writable> localData;
  /** Used to translate Edges during vertex input phase based on localData */
  private final TranslateEdge<I, E> translateEdge;
  /** IPC Client */
  private final WorkerClient<I, V, E> workerClient;
  /** IPC Server */
  private final WorkerServer<I, V, E> workerServer;
  /** Request processor for aggregator requests */
  private final WorkerAggregatorRequestProcessor
  workerAggregatorRequestProcessor;
  /** Master info */
  private MasterInfo masterInfo = new MasterInfo();
  /** List of workers */
  private List<WorkerInfo> workerInfoList = Lists.newArrayList();
  /** Have the partition exchange children (workers) changed? */
  private final BspEvent partitionExchangeChildrenChanged;

  /** Addresses and partitions transfer */
  private BlockingElementsSet<AddressesAndPartitionsWritable>
      addressesAndPartitionsHolder = new BlockingElementsSet<>();

  /** Worker Context */
  private final WorkerContext workerContext;

  /** Handler for aggregators */
  private final WorkerAggregatorHandler globalCommHandler;

  /** Superstep output */
  private final SuperstepOutput<I, V, E> superstepOutput;

  /** array of observers to call back to */
  private final WorkerObserver[] observers;
  /** Writer for worker progress */
  private final WorkerProgressWriter workerProgressWriter;

  // Per-Superstep Metrics
  /** Timer for WorkerContext#postSuperstep */
  private GiraphTimer wcPostSuperstepTimer;
  /** Time spent waiting on requests to finish */
  private GiraphTimer waitRequestsTimer;

  /** InputSplit handlers used in INPUT_SUPERSTEP */
  private final WorkerInputSplitsHandler inputSplitsHandler;

  /** Memory observer */
  private final MemoryObserver memoryObserver;

  /**
   * Constructor for setting up the worker.
   *
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   * @throws IOException
   * @throws InterruptedException
   */
  public BspServiceWorker(
    Mapper<?, ?, ?, ?>.Context context,
    GraphTaskManager<I, V, E> graphTaskManager)
    throws IOException, InterruptedException {
    super(context, graphTaskManager);
    ImmutableClassesGiraphConfiguration<I, V, E> conf = getConfiguration();
    localData = new LocalData<>(conf);
    translateEdge = getConfiguration().edgeTranslationInstance();
    if (translateEdge != null) {
      translateEdge.initialize(this);
    }
    partitionExchangeChildrenChanged = new PredicateLock(context);
    registerBspEvent(partitionExchangeChildrenChanged);
    workerGraphPartitioner =
        getGraphPartitionerFactory().createWorkerGraphPartitioner();
    workerInfo = new WorkerInfo();
    workerServer = new NettyWorkerServer<I, V, E>(conf, this, context,
        graphTaskManager.createUncaughtExceptionHandler());
    workerInfo.setInetSocketAddress(workerServer.getMyAddress(),
        workerServer.getLocalHostOrIp());
    workerInfo.setTaskId(getTaskId());
    workerClient = new NettyWorkerClient<I, V, E>(context, conf, this,
        graphTaskManager.createUncaughtExceptionHandler());
    workerServer.setFlowControl(workerClient.getFlowControl());
    OutOfCoreEngine oocEngine = workerServer.getServerData().getOocEngine();
    if (oocEngine != null) {
      oocEngine.setFlowControl(workerClient.getFlowControl());
    }

    workerAggregatorRequestProcessor =
        new NettyWorkerAggregatorRequestProcessor(getContext(), conf, this);

    globalCommHandler = new WorkerAggregatorHandler(this, conf, context);

    workerContext = conf.createWorkerContext();
    workerContext.setWorkerGlobalCommUsage(globalCommHandler);

    superstepOutput = conf.createSuperstepOutput(context);

    if (conf.isJMapHistogramDumpEnabled()) {
      conf.addWorkerObserverClass(JMapHistoDumper.class);
    }
    if (conf.isReactiveJmapHistogramDumpEnabled()) {
      conf.addWorkerObserverClass(ReactiveJMapHistoDumper.class);
    }
    observers = conf.createWorkerObservers(context);

    WorkerProgress.get().setTaskId(getTaskId());
    workerProgressWriter = conf.trackJobProgressOnClient() ?
        new WorkerProgressWriter(graphTaskManager.getJobProgressTracker()) :
        null;

    GiraphMetrics.get().addSuperstepResetObserver(this);

    inputSplitsHandler = new WorkerInputSplitsHandler(
        workerInfo, masterInfo.getTaskId(), workerClient);

    memoryObserver = new MemoryObserver(getZkExt(), memoryObserverPath, conf);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    waitRequestsTimer = new GiraphTimer(superstepMetrics,
        TIMER_WAIT_REQUESTS, TimeUnit.MICROSECONDS);
    wcPostSuperstepTimer = new GiraphTimer(superstepMetrics,
        "worker-context-post-superstep", TimeUnit.MICROSECONDS);
  }

  @Override
  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public WorkerObserver[] getWorkerObservers() {
    return observers;
  }

  @Override
  public WorkerClient<I, V, E> getWorkerClient() {
    return workerClient;
  }

  public LocalData<I, V, E, ? extends Writable> getLocalData() {
    return localData;
  }

  public TranslateEdge<I, E> getTranslateEdge() {
    return translateEdge;
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
   * Load the vertices/edges from input slits. Do this until all the
   * InputSplits have been processed.
   * All workers will try to do as many InputSplits as they can.  The master
   * will monitor progress and stop this once all the InputSplits have been
   * loaded and check-pointed.  Keep track of the last input split path to
   * ensure the input split cache is flushed prior to marking the last input
   * split complete.
   *
   * Use one or more threads to do the loading.
   *
   * @param inputSplitsCallableFactory Factory for {@link InputSplitsCallable}s
   * @return Statistics of the vertices and edges loaded
   * @throws InterruptedException
   * @throws KeeperException
   */
  private VertexEdgeCount loadInputSplits(
      CallableFactory<VertexEdgeCount> inputSplitsCallableFactory)
    throws KeeperException, InterruptedException {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
    int numThreads = getConfiguration().getNumInputSplitsThreads();
    if (LOG.isInfoEnabled()) {
      LOG.info("loadInputSplits: Using " + numThreads + " thread(s), " +
          "originally " + getConfiguration().getNumInputSplitsThreads() +
          " threads(s)");
    }

    List<VertexEdgeCount> results =
        ProgressableUtils.getResultsWithNCallables(inputSplitsCallableFactory,
            numThreads, "load-%d", getContext());
    for (VertexEdgeCount result : results) {
      vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(result);
    }

    workerClient.waitAllRequests();
    return vertexEdgeCount;
  }

  /**
   * Load the mapping entries from the user-defined
   * {@link org.apache.giraph.io.MappingReader}
   *
   * @return Count of mapping entries loaded
   */
  private long loadMapping() throws KeeperException,
    InterruptedException {
    MappingInputSplitsCallableFactory<I, V, E, ? extends Writable>
        inputSplitsCallableFactory =
        new MappingInputSplitsCallableFactory<>(
            getConfiguration().createWrappedMappingInputFormat(),
            getContext(),
            getConfiguration(),
            this,
            inputSplitsHandler);

    long mappingsLoaded =
        loadInputSplits(inputSplitsCallableFactory).getMappingCount();

    // after all threads finish loading - call postFilling
    localData.getMappingStore().postFilling();
    return mappingsLoaded;
  }

  /**
   * Load the vertices from the user-defined
   * {@link org.apache.giraph.io.VertexReader}
   *
   * @return Count of vertices and edges loaded
   */
  private VertexEdgeCount loadVertices() throws KeeperException,
      InterruptedException {
    VertexInputSplitsCallableFactory<I, V, E> inputSplitsCallableFactory =
        new VertexInputSplitsCallableFactory<I, V, E>(
            getConfiguration().createWrappedVertexInputFormat(),
            getContext(),
            getConfiguration(),
            this,
            inputSplitsHandler);

    return loadInputSplits(inputSplitsCallableFactory);
  }

  /**
   * Load the edges from the user-defined
   * {@link org.apache.giraph.io.EdgeReader}.
   *
   * @return Number of edges loaded
   */
  private long loadEdges() throws KeeperException, InterruptedException {
    EdgeInputSplitsCallableFactory<I, V, E> inputSplitsCallableFactory =
        new EdgeInputSplitsCallableFactory<I, V, E>(
            getConfiguration().createWrappedEdgeInputFormat(),
            getContext(),
            getConfiguration(),
            this,
            inputSplitsHandler);

    return loadInputSplits(inputSplitsCallableFactory).getEdgeCount();
  }

  @Override
  public MasterInfo getMasterInfo() {
    return masterInfo;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return workerInfoList;
  }

  /**
   * Mark current worker as done and then wait for all workers
   * to finish processing input splits.
   */
  private void markCurrentWorkerDoneReadingThenWaitForOthers() {
    String workerInputSplitsDonePath =
        inputSplitsWorkerDonePath + "/" + getWorkerInfo().getHostnameId();
    try {
      getZkExt().createExt(workerInputSplitsDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "markCurrentWorkerDoneThenWaitForOthers: " +
              "KeeperException creating worker done splits", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "markCurrentWorkerDoneThenWaitForOthers: " +
              "InterruptedException creating worker done splits", e);
    }
    while (true) {
      Stat inputSplitsDoneStat;
      try {
        inputSplitsDoneStat =
            getZkExt().exists(inputSplitsAllDonePath, true);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "markCurrentWorkerDoneThenWaitForOthers: " +
                "KeeperException waiting on worker done splits", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "markCurrentWorkerDoneThenWaitForOthers: " +
                "InterruptedException waiting on worker done splits", e);
      }
      if (inputSplitsDoneStat != null) {
        break;
      }
      getInputSplitsAllDoneEvent().waitForTimeoutOrFail(
          GiraphConstants.WAIT_FOR_OTHER_WORKERS_TIMEOUT_MSEC.get(
              getConfiguration()));
      getInputSplitsAllDoneEvent().reset();
    }
  }

  @Override
  public FinishedSuperstepStats setup() {
    // Unless doing a restart, prepare for computation:
    // 1. Start superstep INPUT_SUPERSTEP (no computation)
    // 2. Wait until the INPUT_SPLIT_ALL_READY_PATH node has been created
    // 3. Process input splits until there are no more.
    // 4. Wait until the INPUT_SPLIT_ALL_DONE_PATH node has been created
    // 5. Process any mutations deriving from add edge requests
    // 6. Wait for superstep INPUT_SUPERSTEP to complete.
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      setCachedSuperstep(getRestartedSuperstep());
      return new FinishedSuperstepStats(0, false, 0, 0, true,
          CheckpointStatus.NONE);
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
          return new FinishedSuperstepStats(0, false, 0, 0, true,
              CheckpointStatus.NONE);
        }
      } catch (JSONException e) {
        throw new RuntimeException(
            "setup: Failed to get key-values from " +
                jobState.toString(), e);
      }
    }

    // Add the partitions that this worker owns
    Collection<? extends PartitionOwner> masterSetPartitionOwners =
        startSuperstep();
    workerGraphPartitioner.updatePartitionOwners(
        getWorkerInfo(), masterSetPartitionOwners);
    getPartitionStore().initialize();

/*if[HADOOP_NON_SECURE]
    workerClient.setup();
else[HADOOP_NON_SECURE]*/
    workerClient.setup(getConfiguration().authenticate());
/*end[HADOOP_NON_SECURE]*/

    // Initialize aggregator at worker side during setup.
    // Do this just before vertex and edge loading.
    globalCommHandler.prepareSuperstep(workerAggregatorRequestProcessor);

    VertexEdgeCount vertexEdgeCount;
    long entriesLoaded;

    if (getConfiguration().hasMappingInputFormat()) {
      getContext().progress();
      try {
        entriesLoaded = loadMapping();
        // successfully loaded mapping
        // now initialize graphPartitionerFactory with this data
        getGraphPartitionerFactory().initialize(localData);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadMapping failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadMapping failed with KeeperException", e);
      }
      getContext().progress();
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Finally loaded a total of " +
            entriesLoaded + " entries from inputSplits");
      }

      // Print stats for data stored in localData once mapping is fully
      // loaded on all the workers
      localData.printStats();
    }

    if (getConfiguration().hasVertexInputFormat()) {
      getContext().progress();
      try {
        vertexEdgeCount = loadVertices();
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadVertices failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadVertices failed with KeeperException", e);
      }
      getContext().progress();
    } else {
      vertexEdgeCount = new VertexEdgeCount();
    }
    WorkerProgress.get().finishLoadingVertices();

    if (getConfiguration().hasEdgeInputFormat()) {
      getContext().progress();
      try {
        vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(0, loadEdges());
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadEdges failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadEdges failed with KeeperException", e);
      }
      getContext().progress();
    }
    WorkerProgress.get().finishLoadingEdges();

    if (LOG.isInfoEnabled()) {
      LOG.info("setup: Finally loaded a total of " + vertexEdgeCount);
    }

    markCurrentWorkerDoneReadingThenWaitForOthers();

    // Create remaining partitions owned by this worker.
    for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
      if (partitionOwner.getWorkerInfo().equals(getWorkerInfo()) &&
          !getPartitionStore().hasPartition(
              partitionOwner.getPartitionId())) {
        Partition<I, V, E> partition =
            getConfiguration().createPartition(
                partitionOwner.getPartitionId(), getContext());
        getPartitionStore().addPartition(partition);
      }
    }

    // remove mapping store if possible
    localData.removeMappingStoreIfPossible();

    if (getConfiguration().hasEdgeInputFormat()) {
      // Move edges from temporary storage to their source vertices.
      getServerData().getEdgeStore().moveEdgesToVertices();
    }

    // Generate the partition stats for the input superstep and process
    // if necessary
    List<PartitionStats> partitionStatsList =
        new ArrayList<PartitionStats>();
    PartitionStore<I, V, E> partitionStore = getPartitionStore();
    for (Integer partitionId : partitionStore.getPartitionIds()) {
      PartitionStats partitionStats =
          new PartitionStats(partitionId,
              partitionStore.getPartitionVertexCount(partitionId),
              0,
              partitionStore.getPartitionEdgeCount(partitionId),
              0,
              0,
              workerInfo.getHostnameId());
      partitionStatsList.add(partitionStats);
    }
    workerGraphPartitioner.finalizePartitionStats(
        partitionStatsList, getPartitionStore());

    return finishSuperstep(partitionStatsList, null);
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
      getApplicationAttemptChangedEvent().waitForTimeoutOrFail(
          GiraphConstants.WAIT_ZOOKEEPER_TIMEOUT_MSEC.get(
              getConfiguration()));
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
      getZkExt().deleteExt(myHealthZnode, -1, false);
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
  public Collection<? extends PartitionOwner> startSuperstep() {
    // Algorithm:
    // 1. Communication service will combine message from previous
    //    superstep
    // 2. Register my health for the next superstep.
    // 3. Wait until the partition assignment is complete and get it
    // 4. Get the aggregator values from the previous superstep
    if (getSuperstep() != INPUT_SUPERSTEP) {
      workerServer.prepareSuperstep();
    }

    registerHealth(getSuperstep());

    AddressesAndPartitionsWritable addressesAndPartitions =
        addressesAndPartitionsHolder.getElement(getContext());

    workerInfoList.clear();
    workerInfoList = addressesAndPartitions.getWorkerInfos();
    masterInfo = addressesAndPartitions.getMasterInfo();
    workerServer.resetBytesReceivedPerSuperstep();

    if (LOG.isInfoEnabled()) {
      LOG.info("startSuperstep: " + masterInfo);
    }

    getContext().setStatus("startSuperstep: " +
        getGraphTaskManager().getGraphFunctions().toString() +
        " - Attempt=" + getApplicationAttempt() +
        ", Superstep=" + getSuperstep());

    if (LOG.isDebugEnabled()) {
      LOG.debug("startSuperstep: addressesAndPartitions" +
          addressesAndPartitions.getWorkerInfos());
      for (PartitionOwner partitionOwner : addressesAndPartitions
          .getPartitionOwners()) {
        LOG.debug(partitionOwner.getPartitionId() + " " +
            partitionOwner.getWorkerInfo());
      }
    }

    return addressesAndPartitions.getPartitionOwners();
  }

  @Override
  public FinishedSuperstepStats finishSuperstep(
      List<PartitionStats> partitionStatsList,
      GiraphTimerContext superstepTimerContext) {
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
    // 6. Wait for the master's superstep info, and check if done
    waitForRequestsToFinish();

    getGraphTaskManager().notifyFinishedCommunication();

    long workerSentMessages = 0;
    long workerSentMessageBytes = 0;
    long localVertices = 0;
    for (PartitionStats partitionStats : partitionStatsList) {
      workerSentMessages += partitionStats.getMessagesSentCount();
      workerSentMessageBytes += partitionStats.getMessageBytesSentCount();
      localVertices += partitionStats.getVertexCount();
    }

    if (getSuperstep() != INPUT_SUPERSTEP) {
      postSuperstepCallbacks();
    }

    globalCommHandler.finishSuperstep(workerAggregatorRequestProcessor);

    MessageStore<I, Writable> incomingMessageStore =
        getServerData().getIncomingMessageStore();
    if (incomingMessageStore instanceof AsyncMessageStoreWrapper) {
      ((AsyncMessageStoreWrapper) incomingMessageStore).waitToComplete();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Superstep " + getSuperstep() +
          ", messages = " + workerSentMessages + " " +
          ", message bytes = " + workerSentMessageBytes + " , " +
          MemoryUtils.getRuntimeMemoryStats());
    }

    if (superstepTimerContext != null) {
      superstepTimerContext.stop();
    }
    writeFinshedSuperstepInfoToZK(partitionStatsList,
      workerSentMessages, workerSentMessageBytes);

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "finishSuperstep: (waiting for rest " +
            "of workers) " +
            getGraphTaskManager().getGraphFunctions().toString() +
            " - Attempt=" + getApplicationAttempt() +
            ", Superstep=" + getSuperstep());

    String superstepFinishedNode =
        getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());

    waitForOtherWorkers(superstepFinishedNode);

    GlobalStats globalStats = new GlobalStats();
    SuperstepClasses superstepClasses = SuperstepClasses.createToRead(
        getConfiguration());
    WritableUtils.readFieldsFromZnode(
        getZkExt(), superstepFinishedNode, false, null, globalStats,
        superstepClasses);
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
          " with global stats " + globalStats + " and classes " +
          superstepClasses);
    }
    getContext().setStatus("finishSuperstep: (all workers done) " +
        getGraphTaskManager().getGraphFunctions().toString() +
        " - Attempt=" + getApplicationAttempt() +
        ", Superstep=" + getSuperstep());
    incrCachedSuperstep();
    getConfiguration().updateSuperstepClasses(superstepClasses);

    return new FinishedSuperstepStats(
        localVertices,
        globalStats.getHaltComputation(),
        globalStats.getVertexCount(),
        globalStats.getEdgeCount(),
        false,
        globalStats.getCheckpointStatus());
  }

  /**
   * Handle post-superstep callbacks
   */
  private void postSuperstepCallbacks() {
    GiraphTimerContext timerContext = wcPostSuperstepTimer.time();
    getWorkerContext().postSuperstep();
    timerContext.stop();
    getContext().progress();

    for (WorkerObserver obs : getWorkerObservers()) {
      obs.postSuperstep(getSuperstep());
      getContext().progress();
    }
  }

  /**
   * Wait for all the requests to finish.
   */
  private void waitForRequestsToFinish() {
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Waiting on all requests, superstep " +
          getSuperstep() + " " +
          MemoryUtils.getRuntimeMemoryStats());
    }
    GiraphTimerContext timerContext = waitRequestsTimer.time();
    workerClient.waitAllRequests();
    timerContext.stop();
  }

  /**
   * Wait for all the other Workers to finish the superstep.
   *
   * @param superstepFinishedNode ZooKeeper path to wait on.
   */
  private void waitForOtherWorkers(String superstepFinishedNode) {
    try {
      while (getZkExt().exists(superstepFinishedNode, true) == null) {
        getSuperstepFinishedEvent().waitForTimeoutOrFail(
            GiraphConstants.WAIT_FOR_OTHER_WORKERS_TIMEOUT_MSEC.get(
                getConfiguration()));
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
  }

  /**
   * Write finished superstep info to ZooKeeper.
   *
   * @param partitionStatsList List of partition stats from superstep.
   * @param workerSentMessages Number of messages sent in superstep.
   * @param workerSentMessageBytes Number of message bytes sent
   *                               in superstep.
   */
  private void writeFinshedSuperstepInfoToZK(
      List<PartitionStats> partitionStatsList, long workerSentMessages,
      long workerSentMessageBytes) {
    Collection<PartitionStats> finalizedPartitionStats =
        workerGraphPartitioner.finalizePartitionStats(
            partitionStatsList, getPartitionStore());
    workerClient.sendWritableRequest(masterInfo.getTaskId(),
        new PartitionStatsRequest(finalizedPartitionStats));
    WorkerSuperstepMetrics metrics = new WorkerSuperstepMetrics();
    metrics.readFromRegistry();
    byte[] metricsBytes = WritableUtils.writeToByteArray(metrics);

    JSONObject workerFinishedInfoObj = new JSONObject();
    try {
      workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGES_KEY, workerSentMessages);
      workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGE_BYTES_KEY,
        workerSentMessageBytes);
      workerFinishedInfoObj.put(JSONOBJ_METRICS_KEY,
          Base64.encodeBytes(metricsBytes));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep()) +
        "/" + workerInfo.getHostnameId();
    try {
      getZkExt().createExt(finishedWorkerPath,
          workerFinishedInfoObj.toString().getBytes(Charset.defaultCharset()),
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
  }

  /**
   * Save the vertices using the user-defined VertexOutputFormat from our
   * vertexArray based on the split.
   *
   * @param numLocalVertices Number of local vertices
   * @throws InterruptedException
   */
  private void saveVertices(long numLocalVertices) throws IOException,
      InterruptedException {
    ImmutableClassesGiraphConfiguration<I, V, E>  conf = getConfiguration();

    if (conf.getVertexOutputFormatClass() == null) {
      LOG.warn("saveVertices: " +
          GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS +
          " not specified -- there will be no saved output");
      return;
    }
    if (conf.doOutputDuringComputation()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("saveVertices: The option for doing output during " +
            "computation is selected, so there will be no saving of the " +
            "output in the end of application");
      }
      return;
    }

    final int numPartitions = getPartitionStore().getNumPartitions();
    int numThreads = Math.min(getConfiguration().getNumOutputThreads(),
        numPartitions);
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveVertices: Starting to save " + numLocalVertices + " vertices " +
            "using " + numThreads + " threads");
    final VertexOutputFormat<I, V, E> vertexOutputFormat =
        getConfiguration().createWrappedVertexOutputFormat();
    vertexOutputFormat.preWriting(getContext());

    getPartitionStore().startIteration();

    long verticesToStore = 0;
    PartitionStore<I, V, E> partitionStore = getPartitionStore();
    for (int partitionId : partitionStore.getPartitionIds()) {
      verticesToStore += partitionStore.getPartitionVertexCount(partitionId);
    }
    WorkerProgress.get().startStoring(
        verticesToStore, getPartitionStore().getNumPartitions());

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          /** How often to update WorkerProgress */
          private static final long VERTICES_TO_UPDATE_PROGRESS = 100000;

          @Override
          public Void call() throws Exception {
            VertexWriter<I, V, E> vertexWriter =
                vertexOutputFormat.createVertexWriter(getContext());
            vertexWriter.setConf(getConfiguration());
            vertexWriter.initialize(getContext());
            long nextPrintVertices = 0;
            long nextUpdateProgressVertices = VERTICES_TO_UPDATE_PROGRESS;
            long nextPrintMsecs = System.currentTimeMillis() + 15000;
            int partitionIndex = 0;
            int numPartitions = getPartitionStore().getNumPartitions();
            while (true) {
              Partition<I, V, E> partition =
                  getPartitionStore().getNextPartition();
              if (partition == null) {
                break;
              }

              long verticesWritten = 0;
              for (Vertex<I, V, E> vertex : partition) {
                vertexWriter.writeVertex(vertex);
                ++verticesWritten;

                // Update status at most every 250k vertices or 15 seconds
                if (verticesWritten > nextPrintVertices &&
                    System.currentTimeMillis() > nextPrintMsecs) {
                  LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
                      "saveVertices: Saved " + verticesWritten + " out of " +
                          partition.getVertexCount() + " partition vertices, " +
                          "on partition " + partitionIndex +
                          " out of " + numPartitions);
                  nextPrintMsecs = System.currentTimeMillis() + 15000;
                  nextPrintVertices = verticesWritten + 250000;
                }

                if (verticesWritten >= nextUpdateProgressVertices) {
                  WorkerProgress.get().addVerticesStored(
                      VERTICES_TO_UPDATE_PROGRESS);
                  nextUpdateProgressVertices += VERTICES_TO_UPDATE_PROGRESS;
                }
              }
              getPartitionStore().putPartition(partition);
              ++partitionIndex;
              WorkerProgress.get().addVerticesStored(
                  verticesWritten % VERTICES_TO_UPDATE_PROGRESS);
              WorkerProgress.get().incrementPartitionsStored();
            }
            vertexWriter.close(getContext()); // the temp results are saved now
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "save-vertices-%d", getContext());

    vertexOutputFormat.postWriting(getContext());

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
      "saveVertices: Done saving vertices.");
    // YARN: must complete the commit the "task" output, Hadoop isn't there.
    if (getConfiguration().isPureYarnJob() &&
      getConfiguration().getVertexOutputFormatClass() != null) {
      try {
        OutputCommitter outputCommitter =
          vertexOutputFormat.getOutputCommitter(getContext());
        if (outputCommitter.needsTaskCommit(getContext())) {
          LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
            "OutputCommitter: committing task output.");
          // transfer from temp dirs to "task commit" dirs to prep for
          // the master's OutputCommitter#commitJob(context) call to finish.
          outputCommitter.commitTask(getContext());
        }
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while attempting to obtain " +
          "OutputCommitter.", ie);
      } catch (IOException ioe) {
        LOG.error("Master task's attempt to commit output has " +
          "FAILED.", ioe);
      }
    }
  }

  /**
   * Save the edges using the user-defined EdgeOutputFormat from our
   * vertexArray based on the split.
   *
   * @throws InterruptedException
   */
  private void saveEdges() throws IOException, InterruptedException {
    final ImmutableClassesGiraphConfiguration<I, V, E>  conf =
      getConfiguration();

    if (conf.getEdgeOutputFormatClass() == null) {
      LOG.warn("saveEdges: " +
               GiraphConstants.EDGE_OUTPUT_FORMAT_CLASS +
               "Make sure that the EdgeOutputFormat is not required.");
      return;
    }

    final int numPartitions = getPartitionStore().getNumPartitions();
    int numThreads = Math.min(conf.getNumOutputThreads(),
        numPartitions);
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveEdges: Starting to save the edges using " +
        numThreads + " threads");
    final EdgeOutputFormat<I, V, E> edgeOutputFormat =
        conf.createWrappedEdgeOutputFormat();
    edgeOutputFormat.preWriting(getContext());

    getPartitionStore().startIteration();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            EdgeWriter<I, V, E>  edgeWriter =
                edgeOutputFormat.createEdgeWriter(getContext());
            edgeWriter.setConf(conf);
            edgeWriter.initialize(getContext());

            long nextPrintVertices = 0;
            long nextPrintMsecs = System.currentTimeMillis() + 15000;
            int partitionIndex = 0;
            int numPartitions = getPartitionStore().getNumPartitions();
            while (true) {
              Partition<I, V, E> partition =
                  getPartitionStore().getNextPartition();
              if (partition == null) {
                break;
              }

              long vertices = 0;
              long edges = 0;
              long partitionEdgeCount = partition.getEdgeCount();
              for (Vertex<I, V, E> vertex : partition) {
                for (Edge<I, E> edge : vertex.getEdges()) {
                  edgeWriter.writeEdge(vertex.getId(), vertex.getValue(), edge);
                  ++edges;
                }
                ++vertices;

                // Update status at most every 250k vertices or 15 seconds
                if (vertices > nextPrintVertices &&
                    System.currentTimeMillis() > nextPrintMsecs) {
                  LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
                      "saveEdges: Saved " + edges +
                      " edges out of " + partitionEdgeCount +
                      " partition edges, on partition " + partitionIndex +
                      " out of " + numPartitions);
                  nextPrintMsecs = System.currentTimeMillis() + 15000;
                  nextPrintVertices = vertices + 250000;
                }
              }
              getPartitionStore().putPartition(partition);
              ++partitionIndex;
            }
            edgeWriter.close(getContext()); // the temp results are saved now
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "save-vertices-%d", getContext());

    edgeOutputFormat.postWriting(getContext());

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
      "saveEdges: Done saving edges.");
    // YARN: must complete the commit the "task" output, Hadoop isn't there.
    if (conf.isPureYarnJob() &&
      conf.getVertexOutputFormatClass() != null) {
      try {
        OutputCommitter outputCommitter =
          edgeOutputFormat.getOutputCommitter(getContext());
        if (outputCommitter.needsTaskCommit(getContext())) {
          LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
            "OutputCommitter: committing task output.");
          // transfer from temp dirs to "task commit" dirs to prep for
          // the master's OutputCommitter#commitJob(context) call to finish.
          outputCommitter.commitTask(getContext());
        }
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while attempting to obtain " +
          "OutputCommitter.", ie);
      } catch (IOException ioe) {
        LOG.error("Master task's attempt to commit output has " +
          "FAILED.", ioe);
      }
    }
  }

  @Override
  public void cleanup(FinishedSuperstepStats finishedSuperstepStats)
    throws IOException, InterruptedException {
    workerClient.closeConnections();
    setCachedSuperstep(getSuperstep() - 1);
    if (finishedSuperstepStats.getCheckpointStatus() !=
        CheckpointStatus.CHECKPOINT_AND_HALT) {
      saveVertices(finishedSuperstepStats.getLocalVertexCount());
      saveEdges();
    }
    WorkerProgress.get().finishStoring();
    if (workerProgressWriter != null) {
      workerProgressWriter.stop();
    }
    getPartitionStore().shutdown();
    // All worker processes should denote they are done by adding special
    // znode.  Once the number of znodes equals the number of partitions
    // for workers and masters, the master will clean up the ZooKeeper
    // znodes associated with this job.
    String workerCleanedUpPath = cleanedUpPath  + "/" +
        getTaskId() + WORKER_SUFFIX;
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

    if (getConfiguration().metricsEnabled()) {
      GiraphMetrics.get().dumpToStream(System.err);
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
            getGraphTaskManager().getGraphFunctions().toString() +
            " - Attempt=" + getApplicationAttempt() +
            ", Superstep=" + getSuperstep());

    // Algorithm:
    // For each partition, dump vertices and messages
    Path metadataFilePath = createCheckpointFilePathSafe(
        CheckpointingUtils.CHECKPOINT_METADATA_POSTFIX);
    Path validFilePath = createCheckpointFilePathSafe(
        CheckpointingUtils.CHECKPOINT_VALID_POSTFIX);
    Path checkpointFilePath = createCheckpointFilePathSafe(
        CheckpointingUtils.CHECKPOINT_DATA_POSTFIX);


    // Metadata is buffered and written at the end since it's small and
    // needs to know how many partitions this worker owns
    FSDataOutputStream metadataOutputStream =
        getFs().create(metadataFilePath);
    metadataOutputStream.writeInt(getPartitionStore().getNumPartitions());

    for (Integer partitionId : getPartitionStore().getPartitionIds()) {
      metadataOutputStream.writeInt(partitionId);
    }
    metadataOutputStream.close();

    storeCheckpointVertices();

    FSDataOutputStream checkpointOutputStream =
        getFs().create(checkpointFilePath);
    workerContext.write(checkpointOutputStream);
    getContext().progress();

    // TODO: checkpointing messages along with vertices to avoid multiple loads
    //       of a partition when out-of-core is enabled.
    for (Integer partitionId : getPartitionStore().getPartitionIds()) {
      // write messages
      checkpointOutputStream.writeInt(partitionId);
      getServerData().getCurrentMessageStore()
          .writePartition(checkpointOutputStream, partitionId);
      getContext().progress();

    }

    List<Writable> w2wMessages =
        getServerData().getCurrentWorkerToWorkerMessages();
    WritableUtils.writeList(w2wMessages, checkpointOutputStream);

    checkpointOutputStream.close();

    getFs().createNewFile(validFilePath);

    // Notify master that checkpoint is stored
    String workerWroteCheckpoint =
        getWorkerWroteCheckpointPath(getApplicationAttempt(),
            getSuperstep()) + "/" + workerInfo.getHostnameId();
    try {
      getZkExt().createExt(workerWroteCheckpoint,
          new byte[0],
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("storeCheckpoint: wrote checkpoint worker path " +
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

  /**
   * Create checkpoint file safely. If file already exists remove it first.
   * @param name file extension
   * @return full file path to newly created file
   * @throws IOException
   */
  private Path createCheckpointFilePathSafe(String name) throws IOException {
    Path validFilePath = new Path(getCheckpointBasePath(getSuperstep()) + '.' +
        getWorkerId(workerInfo) + name);
    // Remove these files if they already exist (shouldn't though, unless
    // of previous failure of this worker)
    if (getFs().delete(validFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed " + name + " file " +
          validFilePath);
    }
    return validFilePath;
  }

  /**
   * Returns path to saved checkpoint.
   * Doesn't check if file actually exists.
   * @param superstep saved superstep.
   * @param name extension name
   * @return fill file path to checkpoint file
   */
  private Path getSavedCheckpoint(long superstep, String name) {
    return new Path(getSavedCheckpointBasePath(superstep) + '.' +
        getWorkerId(workerInfo) + name);
  }

  /**
   * Save partitions. To speed up this operation
   * runs in multiple threads.
   */
  private void storeCheckpointVertices() {
    final int numPartitions = getPartitionStore().getNumPartitions();
    int numThreads = Math.min(
        GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfiguration()),
        numPartitions);

    getPartitionStore().startIteration();

    final CompressionCodec codec =
        new CompressionCodecFactory(getConfiguration())
            .getCodec(new Path(
                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                    .get(getConfiguration())));

    long t0 = System.currentTimeMillis();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            while (true) {
              Partition<I, V, E> partition =
                  getPartitionStore().getNextPartition();
              if (partition == null) {
                break;
              }
              Path path =
                  createCheckpointFilePathSafe("_" + partition.getId() +
                      CheckpointingUtils.CHECKPOINT_VERTICES_POSTFIX);

              FSDataOutputStream uncompressedStream =
                  getFs().create(path);


              DataOutputStream stream = codec == null ? uncompressedStream :
                  new DataOutputStream(
                      codec.createOutputStream(uncompressedStream));


              partition.write(stream);

              getPartitionStore().putPartition(partition);

              stream.close();
              uncompressedStream.close();
            }
            return null;
          }


        };
      }
    };

    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "checkpoint-vertices-%d", getContext());

    LOG.info("Save checkpoint in " + (System.currentTimeMillis() - t0) +
        " ms, using " + numThreads + " threads");
  }

  /**
   * Load saved partitions in multiple threads.
   * @param superstep superstep to load
   * @param partitions list of partitions to load
   */
  private void loadCheckpointVertices(final long superstep,
                                      List<Integer> partitions) {
    int numThreads = Math.min(
        GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfiguration()),
        partitions.size());

    final Queue<Integer> partitionIdQueue =
        new ConcurrentLinkedQueue<>(partitions);

    final CompressionCodec codec =
        new CompressionCodecFactory(getConfiguration())
            .getCodec(new Path(
                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                    .get(getConfiguration())));

    long t0 = System.currentTimeMillis();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            while (!partitionIdQueue.isEmpty()) {
              Integer partitionId = partitionIdQueue.poll();
              if (partitionId == null) {
                break;
              }
              Path path =
                  getSavedCheckpoint(superstep, "_" + partitionId +
                      CheckpointingUtils.CHECKPOINT_VERTICES_POSTFIX);

              FSDataInputStream compressedStream =
                  getFs().open(path);

              DataInputStream stream = codec == null ? compressedStream :
                  new DataInputStream(
                      codec.createInputStream(compressedStream));

              Partition<I, V, E> partition =
                  getConfiguration().createPartition(partitionId, getContext());

              partition.readFields(stream);

              getPartitionStore().addPartition(partition);

              stream.close();
            }
            return null;
          }

        };
      }
    };

    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "load-vertices-%d", getContext());

    LOG.info("Loaded checkpoint in " + (System.currentTimeMillis() - t0) +
        " ms, using " + numThreads + " threads");
  }

  @Override
  public VertexEdgeCount loadCheckpoint(long superstep) {
    Path metadataFilePath = getSavedCheckpoint(
        superstep, CheckpointingUtils.CHECKPOINT_METADATA_POSTFIX);

    Path checkpointFilePath = getSavedCheckpoint(
        superstep, CheckpointingUtils.CHECKPOINT_DATA_POSTFIX);
    // Algorithm:
    // Examine all the partition owners and load the ones
    // that match my hostname and id from the master designated checkpoint
    // prefixes.
    try {
      DataInputStream metadataStream =
          getFs().open(metadataFilePath);

      int partitions = metadataStream.readInt();
      List<Integer> partitionIds = new ArrayList<>(partitions);
      for (int i = 0; i < partitions; i++) {
        int partitionId = metadataStream.readInt();
        partitionIds.add(partitionId);
      }

      loadCheckpointVertices(superstep, partitionIds);

      getContext().progress();

      metadataStream.close();

      DataInputStream checkpointStream =
          getFs().open(checkpointFilePath);
      workerContext.readFields(checkpointStream);

      // Load global stats and superstep classes
      GlobalStats globalStats = new GlobalStats();
      SuperstepClasses superstepClasses = SuperstepClasses.createToRead(
          getConfiguration());
      String finalizedCheckpointPath = getSavedCheckpointBasePath(superstep) +
          CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX;
      DataInputStream finalizedStream =
          getFs().open(new Path(finalizedCheckpointPath));
      globalStats.readFields(finalizedStream);
      superstepClasses.readFields(finalizedStream);
      getConfiguration().updateSuperstepClasses(superstepClasses);
      getServerData().resetMessageStores();

      // TODO: checkpointing messages along with vertices to avoid multiple
      //       loads of a partition when out-of-core is enabled.
      for (int i = 0; i < partitions; i++) {
        int partitionId = checkpointStream.readInt();
        getServerData().getCurrentMessageStore()
            .readFieldsForPartition(checkpointStream, partitionId);
      }

      List<Writable> w2wMessages = (List<Writable>) WritableUtils.readList(
          checkpointStream);
      getServerData().getCurrentWorkerToWorkerMessages().addAll(w2wMessages);

      checkpointStream.close();

      if (LOG.isInfoEnabled()) {
        LOG.info("loadCheckpoint: Loaded " +
            workerGraphPartitioner.getPartitionOwners().size() +
            " total.");
      }

      // Communication service needs to setup the connections prior to
      // processing vertices
/*if[HADOOP_NON_SECURE]
      workerClient.setup();
else[HADOOP_NON_SECURE]*/
      workerClient.setup(getConfiguration().authenticate());
/*end[HADOOP_NON_SECURE]*/
      return new VertexEdgeCount(globalStats.getVertexCount(),
          globalStats.getEdgeCount(), 0);

    } catch (IOException e) {
      throw new RuntimeException(
          "loadCheckpoint: Failed for superstep=" + superstep, e);
    }
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
    WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E>(getContext(),
            getConfiguration(), this,
            false /* useOneMessageToManyIdsEncoding */);
    for (Entry<WorkerInfo, List<Integer>> workerPartitionList :
      randomEntryList) {
      for (Integer partitionId : workerPartitionList.getValue()) {
        Partition<I, V, E> partition =
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
            getWorkerInfo(), masterSetPartitionOwners);
    workerClient.openConnections();

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
        getPartitionExchangeChildrenChangedEvent().waitForTimeoutOrFail(
            GiraphConstants.WAIT_FOR_OTHER_WORKERS_TIMEOUT_MSEC.get(
                getConfiguration()));
        getPartitionExchangeChildrenChangedEvent().reset();
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(
          "exchangeVertexPartitions: Got runtime exception", e);
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
      // in YARN, we have to manually commit our own output in 2 stages that we
      // do not have to do in Hadoop-based Giraph. So jsonObj can be null.
      if (getConfiguration().isPureYarnJob() && null == jsonObj) {
        LOG.error("BspServiceWorker#getJobState() came back NULL.");
        return false; // the event has been processed.
      }
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
    } else if (event.getPath().contains(MEMORY_OBSERVER_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      memoryObserver.callGc();
      foundEvent = true;
    }

    return foundEvent;
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public PartitionStore<I, V, E> getPartitionStore() {
    return getServerData().getPartitionStore();
  }

  @Override
  public PartitionOwner getVertexPartitionOwner(I vertexId) {
    return workerGraphPartitioner.getPartitionOwner(vertexId);
  }

  @Override
  public Iterable<? extends PartitionOwner> getPartitionOwners() {
    return workerGraphPartitioner.getPartitionOwners();
  }

  @Override
  public int getPartitionId(I vertexId) {
    PartitionOwner partitionOwner = getVertexPartitionOwner(vertexId);
    return partitionOwner.getPartitionId();
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return getPartitionStore().hasPartition(partitionId);
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    return getPartitionStore().getPartitionIds();
  }

  @Override
  public long getPartitionVertexCount(Integer partitionId) {
    return getPartitionStore().getPartitionVertexCount(partitionId);
  }

  @Override
  public void startIteration() {
    getPartitionStore().startIteration();
  }

  @Override
  public Partition getNextPartition() {
    return getPartitionStore().getNextPartition();
  }

  @Override
  public void putPartition(Partition partition) {
    getPartitionStore().putPartition(partition);
  }

  @Override
  public ServerData<I, V, E> getServerData() {
    return workerServer.getServerData();
  }


  @Override
  public WorkerAggregatorHandler getAggregatorHandler() {
    return globalCommHandler;
  }

  @Override
  public void prepareSuperstep() {
    if (getSuperstep() != INPUT_SUPERSTEP) {
      globalCommHandler.prepareSuperstep(workerAggregatorRequestProcessor);
    }
  }

  @Override
  public SuperstepOutput<I, V, E> getSuperstepOutput() {
    return superstepOutput;
  }

  @Override
  public GlobalStats getGlobalStats() {
    GlobalStats globalStats = new GlobalStats();
    if (getSuperstep() > Math.max(INPUT_SUPERSTEP, getRestartedSuperstep())) {
      String superstepFinishedNode =
          getSuperstepFinishedPath(getApplicationAttempt(),
              getSuperstep() - 1);
      WritableUtils.readFieldsFromZnode(
          getZkExt(), superstepFinishedNode, false, null,
          globalStats);
    }
    return globalStats;
  }

  @Override
  public WorkerInputSplitsHandler getInputSplitsHandler() {
    return inputSplitsHandler;
  }

  @Override
  public void addressesAndPartitionsReceived(
      AddressesAndPartitionsWritable addressesAndPartitions) {
    addressesAndPartitionsHolder.offer(addressesAndPartitions);
  }
}
