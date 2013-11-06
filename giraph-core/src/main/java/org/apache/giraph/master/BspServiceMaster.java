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

package org.apache.giraph.master;

import org.apache.commons.io.FilenameUtils;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.MasterServer;
import org.apache.giraph.comm.netty.NettyMasterClient;
import org.apache.giraph.comm.netty.NettyMasterServer;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.graph.InputSplitPaths;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.AddressesAndPartitionsWritable;
import org.apache.giraph.graph.GraphFunctions;
import org.apache.giraph.graph.InputSplitEvents;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionUtils;
import org.apache.giraph.metrics.AggregatedMetrics;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphTimer;
import org.apache.giraph.metrics.GiraphTimerContext;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.metrics.WorkerSuperstepMetrics;
import org.apache.giraph.utils.JMapHistoDumper;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.utils.LogStacktraceCallable;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.iharder.Base64;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.giraph.conf.GiraphConstants.INPUT_SPLIT_SAMPLE_PERCENT;
import static org.apache.giraph.conf.GiraphConstants.KEEP_ZOOKEEPER_DATA;
import static org.apache.giraph.conf.GiraphConstants.PARTITION_LONG_TAIL_MIN_PRINT;
import static org.apache.giraph.conf.GiraphConstants.USE_INPUT_SPLIT_LOCALITY;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceMaster}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class BspServiceMaster<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends BspService<I, V, E>
    implements CentralizedServiceMaster<I, V, E>,
    ResetSuperstepMetricsObserver {
  /** Print worker names only if there are 10 workers left */
  public static final int MAX_PRINTABLE_REMAINING_WORKERS = 10;
  /** How many threads to use when writing input splits to zookeeper*/
  public static final String NUM_MASTER_ZK_INPUT_SPLIT_THREADS =
      "giraph.numMasterZkInputSplitThreads";
  /** Default number of threads to use when writing input splits to zookeeper */
  public static final int DEFAULT_INPUT_SPLIT_THREAD_COUNT = 1;
  /** Time instance to use for timing */
  private static final Time TIME = SystemTime.get();
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
  /** Am I the master? */
  private boolean isMaster = false;
  /** Max number of workers */
  private final int maxWorkers;
  /** Min number of workers */
  private final int minWorkers;
  /** Max number of supersteps */
  private final int maxNumberOfSupersteps;
  /** Min % responded workers */
  private final float minPercentResponded;
  /** Msecs to wait for an event */
  private final int eventWaitMsecs;
  /** Max msecs to wait for a superstep to get enough workers */
  private final int maxSuperstepWaitMsecs;
  /** Min number of long tails before printing */
  private final int partitionLongTailMinPrint;
  /** Last finalized checkpoint */
  private long lastCheckpointedSuperstep = -1;
  /** Worker wrote checkpoint */
  private final BspEvent workerWroteCheckpoint;
  /** State of the superstep changed */
  private final BspEvent superstepStateChanged;
  /** Master graph partitioner */
  private final MasterGraphPartitioner<I, V, E> masterGraphPartitioner;
  /** All the partition stats from the last superstep */
  private final List<PartitionStats> allPartitionStatsList =
      new ArrayList<PartitionStats>();
  /** Handler for aggregators */
  private MasterAggregatorHandler aggregatorHandler;
  /** Master class */
  private MasterCompute masterCompute;
  /** IPC Client */
  private MasterClient masterClient;
  /** IPC Server */
  private MasterServer masterServer;
  /** Master info */
  private MasterInfo masterInfo;
  /** List of workers in current superstep */
  private List<WorkerInfo> chosenWorkerInfoList = Lists.newArrayList();
  /** Limit locality information added to each InputSplit znode */
  private final int localityLimit = 5;
  /** Observers over master lifecycle. */
  private final MasterObserver[] observers;

  // Per-Superstep Metrics
  /** MasterCompute time */
  private GiraphTimer masterComputeTimer;

  /**
   * Constructor for setting up the master.
   *
   * @param sessionMsecTimeout Msecs to timeout connecting to ZooKeeper
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   */
  public BspServiceMaster(
      int sessionMsecTimeout,
      Mapper<?, ?, ?, ?>.Context context,
      GraphTaskManager<I, V, E> graphTaskManager) {
    super(sessionMsecTimeout, context, graphTaskManager);
    workerWroteCheckpoint = new PredicateLock(context);
    registerBspEvent(workerWroteCheckpoint);
    superstepStateChanged = new PredicateLock(context);
    registerBspEvent(superstepStateChanged);

    ImmutableClassesGiraphConfiguration<I, V, E> conf =
        getConfiguration();

    maxWorkers = conf.getMaxWorkers();
    minWorkers = conf.getMinWorkers();
    maxNumberOfSupersteps = conf.getMaxNumberOfSupersteps();
    minPercentResponded = GiraphConstants.MIN_PERCENT_RESPONDED.get(conf);
    eventWaitMsecs = conf.getEventWaitMsecs();
    maxSuperstepWaitMsecs = conf.getMaxMasterSuperstepWaitMsecs();
    partitionLongTailMinPrint = PARTITION_LONG_TAIL_MIN_PRINT.get(conf);
    masterGraphPartitioner =
        getGraphPartitionerFactory().createMasterGraphPartitioner();
    if (conf.isJMapHistogramDumpEnabled()) {
      conf.addMasterObserverClass(JMapHistoDumper.class);
    }
    observers = conf.createMasterObservers();

    GiraphMetrics.get().addSuperstepResetObserver(this);
    GiraphStats.init(context);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    masterComputeTimer = new GiraphTimer(superstepMetrics,
        "master-compute-call", TimeUnit.MILLISECONDS);
  }

  @Override
  public void setJobState(ApplicationState state,
      long applicationAttempt,
      long desiredSuperstep) {
    setJobState(state, applicationAttempt, desiredSuperstep, true);
  }

  /**
   * Set the job state.
   *
   * @param state State of the application.
   * @param applicationAttempt Attempt to start on
   * @param desiredSuperstep Superstep to restart from (if applicable)
   * @param killJobOnFailure if true, and the desired state is FAILED,
   *                         then kill this job.
   */
  private void setJobState(ApplicationState state,
      long applicationAttempt,
      long desiredSuperstep,
      boolean killJobOnFailure) {
    JSONObject jobState = new JSONObject();
    try {
      jobState.put(JSONOBJ_STATE_KEY, state.toString());
      jobState.put(JSONOBJ_APPLICATION_ATTEMPT_KEY, applicationAttempt);
      jobState.put(JSONOBJ_SUPERSTEP_KEY, desiredSuperstep);
    } catch (JSONException e) {
      throw new RuntimeException("setJobState: Couldn't put " +
          state.toString());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("setJobState: " + jobState.toString() + " on superstep " +
          getSuperstep());
    }
    try {
      getZkExt().createExt(masterJobStatePath + "/jobState",
          jobState.toString().getBytes(Charset.defaultCharset()),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL,
          true);
    } catch (KeeperException.NodeExistsException e) {
      throw new IllegalStateException(
          "setJobState: Imposible that " +
              masterJobStatePath + " already exists!", e);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "setJobState: Unknown KeeperException for " +
              masterJobStatePath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "setJobState: Unknown InterruptedException for " +
              masterJobStatePath, e);
    }
    if (state == ApplicationState.FAILED && killJobOnFailure) {
      failJob(new IllegalStateException("FAILED"));
    }

  }

  /**
   * Set the job state to FAILED. This will kill the job, and log exceptions to
   * any observers.
   *
   * @param reason The reason the job failed
   */
  private void setJobStateFailed(String reason) {
    setJobState(ApplicationState.FAILED, -1, -1, false);
    failJob(new IllegalStateException(reason));
  }

  /**
   * Common method for generating vertex/edge input splits.
   *
   * @param inputFormat The vertex/edge input format
   * @param minSplitCountHint Minimum number of splits to create (hint)
   * @param inputSplitType Type of input splits (for logging purposes)
   * @return List of input splits for the given format
   */
  private List<InputSplit> generateInputSplits(GiraphInputFormat inputFormat,
                                               int minSplitCountHint,
                                               String inputSplitType) {
    String logPrefix = "generate" + inputSplitType + "InputSplits";
    List<InputSplit> splits;
    try {
      splits = inputFormat.getSplits(getContext(), minSplitCountHint);
    } catch (IOException e) {
      throw new IllegalStateException(logPrefix + ": Got IOException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          logPrefix + ": Got InterruptedException", e);
    }
    float samplePercent =
        INPUT_SPLIT_SAMPLE_PERCENT.get(getConfiguration());
    if (samplePercent != INPUT_SPLIT_SAMPLE_PERCENT.getDefaultValue()) {
      int lastIndex = (int) (samplePercent * splits.size() / 100f);
      List<InputSplit> sampleSplits = splits.subList(0, lastIndex);
      LOG.warn(logPrefix + ": Using sampling - Processing only " +
          sampleSplits.size() + " instead of " + splits.size() +
          " expected splits.");
      return sampleSplits;
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info(logPrefix + ": Got " + splits.size() +
            " input splits for " + minSplitCountHint + " input threads");
      }
      return splits;
    }
  }

  /**
   * When there is no salvaging this job, fail it.
   *
   * @param e Exception to log to observers
   */
  private void failJob(Exception e) {
    LOG.fatal("failJob: Killing job " + getJobId());
    LOG.fatal("failJob: exception " + e.toString());
    try {
      if (getConfiguration().isPureYarnJob()) {
        throw new RuntimeException(
          "BspServiceMaster (YARN profile) is " +
          "FAILING this task, throwing exception to end job run.", e);
      } else {
        @SuppressWarnings("deprecation")
        org.apache.hadoop.mapred.JobClient jobClient =
          new org.apache.hadoop.mapred.JobClient(
            (org.apache.hadoop.mapred.JobConf)
            getContext().getConfiguration());
        @SuppressWarnings("deprecation")
        JobID jobId = JobID.forName(getJobId());
        RunningJob job = jobClient.getJob(jobId);
        job.killJob();
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      failureCleanup(e);
    }
  }

  /**
   * Parse the {@link WorkerInfo} objects from a ZooKeeper path
   * (and children).
   *
   * @param workerInfosPath Path where all the workers are children
   * @param watch Watch or not?
   * @return List of workers in that path
   */
  private List<WorkerInfo> getWorkerInfosFromPath(String workerInfosPath,
      boolean watch) {
    List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>();
    List<String> workerInfoPathList;
    try {
      workerInfoPathList =
          getZkExt().getChildrenExt(workerInfosPath, watch, false, true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getWorkers: Got KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getWorkers: Got InterruptedStateException", e);
    }
    for (String workerInfoPath : workerInfoPathList) {
      WorkerInfo workerInfo = new WorkerInfo();
      WritableUtils.readFieldsFromZnode(
          getZkExt(), workerInfoPath, true, null, workerInfo);
      workerInfoList.add(workerInfo);
    }
    return workerInfoList;
  }

  /**
   * Get the healthy and unhealthy {@link WorkerInfo} objects for
   * a superstep
   *
   * @param superstep superstep to check
   * @param healthyWorkerInfoList filled in with current data
   * @param unhealthyWorkerInfoList filled in with current data
   */
  private void getAllWorkerInfos(
      long superstep,
      List<WorkerInfo> healthyWorkerInfoList,
      List<WorkerInfo> unhealthyWorkerInfoList) {
    String healthyWorkerInfoPath =
        getWorkerInfoHealthyPath(getApplicationAttempt(), superstep);
    String unhealthyWorkerInfoPath =
        getWorkerInfoUnhealthyPath(getApplicationAttempt(), superstep);

    try {
      getZkExt().createOnceExt(healthyWorkerInfoPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException("getWorkers: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("getWorkers: InterruptedException", e);
    }

    try {
      getZkExt().createOnceExt(unhealthyWorkerInfoPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException("getWorkers: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("getWorkers: InterruptedException", e);
    }

    List<WorkerInfo> currentHealthyWorkerInfoList =
        getWorkerInfosFromPath(healthyWorkerInfoPath, true);
    List<WorkerInfo> currentUnhealthyWorkerInfoList =
        getWorkerInfosFromPath(unhealthyWorkerInfoPath, false);

    healthyWorkerInfoList.clear();
    if (currentHealthyWorkerInfoList != null) {
      for (WorkerInfo healthyWorkerInfo :
        currentHealthyWorkerInfoList) {
        healthyWorkerInfoList.add(healthyWorkerInfo);
      }
    }

    unhealthyWorkerInfoList.clear();
    if (currentUnhealthyWorkerInfoList != null) {
      for (WorkerInfo unhealthyWorkerInfo :
        currentUnhealthyWorkerInfoList) {
        unhealthyWorkerInfoList.add(unhealthyWorkerInfo);
      }
    }
  }

  /**
   * Check all the {@link WorkerInfo} objects to ensure that a minimum
   * number of good workers exists out of the total that have reported.
   *
   * @return List of of healthy workers such that the minimum has been
   *         met, otherwise null
   */
  private List<WorkerInfo> checkWorkers() {
    boolean failJob = true;
    long failWorkerCheckMsecs =
        SystemTime.get().getMilliseconds() + maxSuperstepWaitMsecs;
    List<WorkerInfo> healthyWorkerInfoList = new ArrayList<WorkerInfo>();
    List<WorkerInfo> unhealthyWorkerInfoList = new ArrayList<WorkerInfo>();
    int totalResponses = -1;
    while (SystemTime.get().getMilliseconds() < failWorkerCheckMsecs) {
      getContext().progress();
      getAllWorkerInfos(
          getSuperstep(), healthyWorkerInfoList, unhealthyWorkerInfoList);
      totalResponses = healthyWorkerInfoList.size() +
          unhealthyWorkerInfoList.size();
      if ((totalResponses * 100.0f / maxWorkers) >=
          minPercentResponded) {
        failJob = false;
        break;
      }
      getContext().setStatus(getGraphTaskManager().getGraphFunctions() + " " +
          "checkWorkers: Only found " +
          totalResponses +
          " responses of " + maxWorkers +
          " needed to start superstep " +
          getSuperstep());
      if (getWorkerHealthRegistrationChangedEvent().waitMsecs(
          eventWaitMsecs)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("checkWorkers: Got event that health " +
              "registration changed, not using poll attempt");
        }
        getWorkerHealthRegistrationChangedEvent().reset();
        continue;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("checkWorkers: Only found " + totalResponses +
            " responses of " + maxWorkers +
            " needed to start superstep " +
            getSuperstep() + ".  Reporting every " +
            eventWaitMsecs + " msecs, " +
            (failWorkerCheckMsecs - SystemTime.get().getMilliseconds()) +
            " more msecs left before giving up.");
        // Find the missing workers if there are only a few
        if ((maxWorkers - totalResponses) <=
            partitionLongTailMinPrint) {
          logMissingWorkersOnSuperstep(healthyWorkerInfoList,
              unhealthyWorkerInfoList);
        }
      }
    }
    if (failJob) {
      LOG.error("checkWorkers: Did not receive enough processes in " +
          "time (only " + totalResponses + " of " +
          minWorkers + " required) after waiting " + maxSuperstepWaitMsecs +
          "msecs).  This occurs if you do not have enough map tasks " +
          "available simultaneously on your Hadoop instance to fulfill " +
          "the number of requested workers.");
      return null;
    }

    if (healthyWorkerInfoList.size() < minWorkers) {
      LOG.error("checkWorkers: Only " + healthyWorkerInfoList.size() +
          " available when " + minWorkers + " are required.");
      logMissingWorkersOnSuperstep(healthyWorkerInfoList,
          unhealthyWorkerInfoList);
      return null;
    }

    getContext().setStatus(getGraphTaskManager().getGraphFunctions() + " " +
        "checkWorkers: Done - Found " + totalResponses +
        " responses of " + maxWorkers + " needed to start superstep " +
        getSuperstep());

    return healthyWorkerInfoList;
  }

  /**
   * Log info level of the missing workers on the superstep
   *
   * @param healthyWorkerInfoList Healthy worker list
   * @param unhealthyWorkerInfoList Unhealthy worker list
   */
  private void logMissingWorkersOnSuperstep(
      List<WorkerInfo> healthyWorkerInfoList,
      List<WorkerInfo> unhealthyWorkerInfoList) {
    if (LOG.isInfoEnabled()) {
      Set<Integer> partitionSet = new TreeSet<Integer>();
      for (WorkerInfo workerInfo : healthyWorkerInfoList) {
        partitionSet.add(workerInfo.getTaskId());
      }
      for (WorkerInfo workerInfo : unhealthyWorkerInfoList) {
        partitionSet.add(workerInfo.getTaskId());
      }
      for (int i = 1; i <= maxWorkers; ++i) {
        if (partitionSet.contains(Integer.valueOf(i))) {
          continue;
        } else if (i == getTaskPartition()) {
          continue;
        } else {
          LOG.info("logMissingWorkersOnSuperstep: No response from " +
              "partition " + i + " (could be master)");
        }
      }
    }
  }

  /**
   * Common method for creating vertex/edge input splits.
   *
   * @param inputFormat The vertex/edge input format
   * @param inputSplitPaths ZooKeeper input split paths
   * @param inputSplitType Type of input split (for logging purposes)
   * @return Number of splits. Returns -1 on failure to create
   *         valid input splits.
   */
  private int createInputSplits(GiraphInputFormat inputFormat,
                                InputSplitPaths inputSplitPaths,
                                String inputSplitType) {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();
    String logPrefix = "create" + inputSplitType + "InputSplits";
    // Only the 'master' should be doing this.  Wait until the number of
    // processes that have reported health exceeds the minimum percentage.
    // If the minimum percentage is not met, fail the job.  Otherwise
    // generate the input splits
    String inputSplitsPath = inputSplitPaths.getPath();
    try {
      if (getZkExt().exists(inputSplitsPath, false) != null) {
        LOG.info(inputSplitsPath + " already exists, no need to create");
        return Integer.parseInt(
            new String(getZkExt().getData(inputSplitsPath, false, null),
                Charset.defaultCharset()));
      }
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info(logPrefix + ": Need to create the input splits at " +
            inputSplitsPath);
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(logPrefix + ": KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(logPrefix + ": InterruptedException", e);
    }

    // When creating znodes, in case the master has already run, resume
    // where it left off.
    List<WorkerInfo> healthyWorkerInfoList = checkWorkers();
    if (healthyWorkerInfoList == null) {
      setJobStateFailed("Not enough healthy workers to create input splits");
      return -1;
    }

    // Create at least as many splits as the total number of input threads.
    int minSplitCountHint = healthyWorkerInfoList.size() *
        conf.getNumInputSplitsThreads();

    // Note that the input splits may only be a sample if
    // INPUT_SPLIT_SAMPLE_PERCENT is set to something other than 100
    List<InputSplit> splitList = generateInputSplits(inputFormat,
        minSplitCountHint, inputSplitType);

    if (splitList.isEmpty()) {
      LOG.fatal(logPrefix + ": Failing job due to 0 input splits, " +
          "check input of " + inputFormat.getClass().getName() + "!");
      getContext().setStatus("Failing job due to 0 input splits, " +
          "check input of " + inputFormat.getClass().getName() + "!");
      setJobStateFailed("0 input splits");
    }
    if (minSplitCountHint > splitList.size()) {
      LOG.warn(logPrefix + ": Number of inputSplits=" +
          splitList.size() + " < " +
          minSplitCountHint +
          "=total number of input threads, " +
          "some threads will be not used");
    }

    // Write input splits to zookeeper in parallel
    int inputSplitThreadCount = conf.getInt(NUM_MASTER_ZK_INPUT_SPLIT_THREADS,
        DEFAULT_INPUT_SPLIT_THREAD_COUNT);
    if (LOG.isInfoEnabled()) {
      LOG.info(logPrefix + ": Starting to write input split data " +
          "to zookeeper with " + inputSplitThreadCount + " threads");
    }
    ExecutorService taskExecutor =
        Executors.newFixedThreadPool(inputSplitThreadCount);
    boolean writeLocations = USE_INPUT_SPLIT_LOCALITY.get(conf);
    for (int i = 0; i < splitList.size(); ++i) {
      InputSplit inputSplit = splitList.get(i);
      taskExecutor.submit(new LogStacktraceCallable<Void>(
          new WriteInputSplit(inputFormat, inputSplit, inputSplitsPath, i,
              writeLocations)));
    }
    taskExecutor.shutdown();
    ProgressableUtils.awaitExecutorTermination(taskExecutor, getContext());
    if (LOG.isInfoEnabled()) {
      LOG.info(logPrefix + ": Done writing input split data to zookeeper");
    }

    // Let workers know they can start trying to load the input splits
    try {
      getZkExt().createExt(inputSplitPaths.getAllReadyPath(),
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          false);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info(logPrefix + ": Node " +
          inputSplitPaths.getAllReadyPath() + " already exists.");
    } catch (KeeperException e) {
      throw new IllegalStateException(logPrefix + ": KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(logPrefix + ": IllegalStateException", e);
    }

    return splitList.size();
  }

  @Override
  public int createVertexInputSplits() {
    // Short-circuit if there is no vertex input format
    if (!getConfiguration().hasVertexInputFormat()) {
      return 0;
    }
    VertexInputFormat<I, V, E> vertexInputFormat =
        getConfiguration().createWrappedVertexInputFormat();
    return createInputSplits(vertexInputFormat, vertexInputSplitsPaths,
        "Vertex");
  }

  @Override
  public int createEdgeInputSplits() {
    // Short-circuit if there is no edge input format
    if (!getConfiguration().hasEdgeInputFormat()) {
      return 0;
    }
    EdgeInputFormat<I, E> edgeInputFormat =
        getConfiguration().createWrappedEdgeInputFormat();
    return createInputSplits(edgeInputFormat, edgeInputSplitsPaths,
        "Edge");
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return chosenWorkerInfoList;
  }

  @Override
  public MasterAggregatorHandler getAggregatorHandler() {
    return aggregatorHandler;
  }

  @Override
  public MasterCompute getMasterCompute() {
    return masterCompute;
  }

  /**
   * Read the finalized checkpoint file and associated metadata files for the
   * checkpoint.  Modifies the {@link PartitionOwner} objects to get the
   * checkpoint prefixes.  It is an optimization to prevent all workers from
   * searching all the files.  Also read in the aggregator data from the
   * finalized checkpoint file and setting it.
   *
   * @param superstep Checkpoint set to examine.
   * @param partitionOwners Partition owners to modify with checkpoint
   *        prefixes
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void prepareCheckpointRestart(
    long superstep,
    Collection<PartitionOwner> partitionOwners)
    throws IOException, KeeperException, InterruptedException {
    FileSystem fs = getFs();
    List<Path> validMetadataPathList = new ArrayList<Path>();
    String finalizedCheckpointPath =
        getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
    DataInputStream finalizedStream =
        fs.open(new Path(finalizedCheckpointPath));
    GlobalStats globalStats = new GlobalStats();
    globalStats.readFields(finalizedStream);
    updateCounters(globalStats);
    SuperstepClasses superstepClasses = new SuperstepClasses();
    superstepClasses.readFields(finalizedStream);
    getConfiguration().updateSuperstepClasses(superstepClasses);
    int prefixFileCount = finalizedStream.readInt();
    for (int i = 0; i < prefixFileCount; ++i) {
      String metadataFilePath =
          finalizedStream.readUTF() + CHECKPOINT_METADATA_POSTFIX;
      validMetadataPathList.add(new Path(metadataFilePath));
    }

    aggregatorHandler.readFields(finalizedStream);
    masterCompute.readFields(finalizedStream);
    finalizedStream.close();

    Map<Integer, PartitionOwner> idOwnerMap =
        new HashMap<Integer, PartitionOwner>();
    for (PartitionOwner partitionOwner : partitionOwners) {
      if (idOwnerMap.put(partitionOwner.getPartitionId(),
          partitionOwner) != null) {
        throw new IllegalStateException(
            "prepareCheckpointRestart: Duplicate partition " +
                partitionOwner);
      }
    }
    // Reading the metadata files.  Simply assign each partition owner
    // the correct file prefix based on the partition id.
    for (Path metadataPath : validMetadataPathList) {
      String checkpointFilePrefix = metadataPath.toString();
      checkpointFilePrefix =
          checkpointFilePrefix.substring(
              0,
              checkpointFilePrefix.length() -
              CHECKPOINT_METADATA_POSTFIX.length());
      DataInputStream metadataStream = fs.open(metadataPath);
      long partitions = metadataStream.readInt();
      for (long i = 0; i < partitions; ++i) {
        long dataPos = metadataStream.readLong();
        int partitionId = metadataStream.readInt();
        PartitionOwner partitionOwner = idOwnerMap.get(partitionId);
        if (LOG.isInfoEnabled()) {
          LOG.info("prepareSuperstepRestart: File " + metadataPath +
              " with position " + dataPos +
              ", partition id = " + partitionId +
              " assigned to " + partitionOwner);
        }
        partitionOwner.setCheckpointFilesPrefix(checkpointFilePrefix);
      }
      metadataStream.close();
    }
  }

  @Override
  public void setup() {
    // Might have to manually load a checkpoint.
    // In that case, the input splits are not set, they will be faked by
    // the checkpoint files.  Each checkpoint file will be an input split
    // and the input split

    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      GiraphStats.getInstance().getSuperstepCounter().
        setValue(getRestartedSuperstep());
    }
    for (MasterObserver observer : observers) {
      observer.preApplication();
      getContext().progress();
    }
  }

  @Override
  public boolean becomeMaster() {
    // Create my bid to become the master, then try to become the worker
    // or return false.
    String myBid = null;
    try {
      myBid =
          getZkExt().createExt(masterElectionPath +
              "/" + getHostnamePartitionId(),
              null,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL_SEQUENTIAL,
              true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "becomeMaster: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "becomeMaster: IllegalStateException", e);
    }
    while (true) {
      JSONObject jobState = getJobState();
      try {
        if ((jobState != null) &&
            ApplicationState.valueOf(
                jobState.getString(JSONOBJ_STATE_KEY)) ==
                ApplicationState.FINISHED) {
          LOG.info("becomeMaster: Job is finished, " +
              "give up trying to be the master!");
          isMaster = false;
          return isMaster;
        }
      } catch (JSONException e) {
        throw new IllegalStateException(
            "becomeMaster: Couldn't get state from " + jobState, e);
      }
      try {
        List<String> masterChildArr =
            getZkExt().getChildrenExt(
                masterElectionPath, true, true, true);
        if (LOG.isInfoEnabled()) {
          LOG.info("becomeMaster: First child is '" +
              masterChildArr.get(0) + "' and my bid is '" +
              myBid + "'");
        }
        if (masterChildArr.get(0).equals(myBid)) {
          GiraphStats.getInstance().getCurrentMasterTaskPartition().
              setValue(getTaskPartition());
          aggregatorHandler = new MasterAggregatorHandler(getConfiguration(),
              getContext());
          aggregatorHandler.initialize(this);
          masterCompute = getConfiguration().createMasterCompute();
          masterCompute.setMasterAggregatorUsage(aggregatorHandler);

          masterInfo = new MasterInfo();
          masterServer =
              new NettyMasterServer(getConfiguration(), this, getContext());
          masterInfo.setInetSocketAddress(masterServer.getMyAddress());
          masterInfo.setTaskId(getTaskPartition());
          masterClient =
              new NettyMasterClient(getContext(), getConfiguration(), this);

          if (LOG.isInfoEnabled()) {
            LOG.info("becomeMaster: I am now the master!");
          }
          isMaster = true;
          return isMaster;
        }
        LOG.info("becomeMaster: Waiting to become the master...");
        getMasterElectionChildrenChangedEvent().waitForever();
        getMasterElectionChildrenChangedEvent().reset();
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "becomeMaster: KeeperException", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "becomeMaster: IllegalStateException", e);
      }
    }
  }

  @Override
  public MasterInfo getMasterInfo() {
    return masterInfo;
  }

  /**
   * Collect and aggregate the worker statistics for a particular superstep.
   *
   * @param superstep Superstep to aggregate on
   * @return Global statistics aggregated on all worker statistics
   */
  private GlobalStats aggregateWorkerStats(long superstep) {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();

    Class<? extends PartitionStats> partitionStatsClass =
        masterGraphPartitioner.createPartitionStats().getClass();
    GlobalStats globalStats = new GlobalStats();
    // Get the stats from the all the worker selected nodes
    String workerFinishedPath =
        getWorkerFinishedPath(getApplicationAttempt(), superstep);
    List<String> workerFinishedPathList = null;
    try {
      workerFinishedPathList =
          getZkExt().getChildrenExt(
              workerFinishedPath, false, false, true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "aggregateWorkerStats: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "aggregateWorkerStats: InterruptedException", e);
    }

    AggregatedMetrics aggregatedMetrics = new AggregatedMetrics();

    allPartitionStatsList.clear();
    for (String finishedPath : workerFinishedPathList) {
      String hostnamePartitionId = FilenameUtils.getName(finishedPath);
      JSONObject workerFinishedInfoObj = null;
      try {
        byte [] zkData =
            getZkExt().getData(finishedPath, false, null);
        workerFinishedInfoObj = new JSONObject(new String(zkData,
            Charset.defaultCharset()));
        List<PartitionStats> statsList =
            WritableUtils.readListFieldsFromByteArray(
                Base64.decode(workerFinishedInfoObj.getString(
                    JSONOBJ_PARTITION_STATS_KEY)),
                    partitionStatsClass,
                    conf);
        for (PartitionStats partitionStats : statsList) {
          globalStats.addPartitionStats(partitionStats);
          allPartitionStatsList.add(partitionStats);
        }
        globalStats.addMessageCount(
            workerFinishedInfoObj.getLong(
                JSONOBJ_NUM_MESSAGES_KEY));
        globalStats.addMessageBytesCount(
          workerFinishedInfoObj.getLong(
              JSONOBJ_NUM_MESSAGE_BYTES_KEY));
        if (conf.metricsEnabled() &&
            workerFinishedInfoObj.has(JSONOBJ_METRICS_KEY)) {
          WorkerSuperstepMetrics workerMetrics = new WorkerSuperstepMetrics();
          WritableUtils.readFieldsFromByteArray(
              Base64.decode(
                  workerFinishedInfoObj.getString(
                      JSONOBJ_METRICS_KEY)),
              workerMetrics);
          aggregatedMetrics.add(workerMetrics, hostnamePartitionId);
        }
      } catch (JSONException e) {
        throw new IllegalStateException(
            "aggregateWorkerStats: JSONException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "aggregateWorkerStats: KeeperException", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "aggregateWorkerStats: InterruptedException", e);
      } catch (IOException e) {
        throw new IllegalStateException(
            "aggregateWorkerStats: IOException", e);
      }
    }

    if (conf.metricsEnabled()) {
      if (GiraphConstants.METRICS_DIRECTORY.isDefaultValue(conf)) {
        aggregatedMetrics.print(superstep, System.err);
      } else {
        printAggregatedMetricsToHDFS(superstep, aggregatedMetrics);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("aggregateWorkerStats: Aggregation found " + globalStats +
          " on superstep = " + getSuperstep());
    }
    return globalStats;
  }

  /**
   * Write superstep metrics to own file in HDFS
   * @param superstep the current superstep
   * @param aggregatedMetrics the aggregated metrics to write
   */
  private void printAggregatedMetricsToHDFS(
      long superstep, AggregatedMetrics aggregatedMetrics) {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();
    PrintStream out = null;
    Path dir = new Path(GiraphConstants.METRICS_DIRECTORY.get(conf));
    Path outFile = new Path(GiraphConstants.METRICS_DIRECTORY.get(conf) +
        Path.SEPARATOR_CHAR + "superstep_" + superstep + ".metrics");
    try {
      FileSystem fs;
      fs = FileSystem.get(conf);
      if (!fs.exists(dir)) {
        fs.mkdirs(dir);
      }
      if (fs.exists(outFile)) {
        throw new RuntimeException(
            "printAggregatedMetricsToHDFS: metrics file exists");
      }
      out = new PrintStream(fs.create(outFile), false,
          Charset.defaultCharset().name());
      aggregatedMetrics.print(superstep, out);
    } catch (IOException e) {
      throw new RuntimeException(
          "printAggregatedMetricsToHDFS: error creating metrics file", e);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Finalize the checkpoint file prefixes by taking the chosen workers and
   * writing them to a finalized file.  Also write out the master
   * aggregated aggregator array from the previous superstep.
   *
   * @param superstep superstep to finalize
   * @param chosenWorkerInfoList list of chosen workers that will be finalized
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void finalizeCheckpoint(long superstep,
    List<WorkerInfo> chosenWorkerInfoList)
    throws IOException, KeeperException, InterruptedException {
    Path finalizedCheckpointPath =
        new Path(getCheckpointBasePath(superstep) +
            CHECKPOINT_FINALIZED_POSTFIX);
    try {
      getFs().delete(finalizedCheckpointPath, false);
    } catch (IOException e) {
      LOG.warn("finalizedValidCheckpointPrefixes: Removed old file " +
          finalizedCheckpointPath);
    }

    // Format:
    // <global statistics>
    // <superstep classes>
    // <number of files>
    // <used file prefix 0><used file prefix 1>...
    // <aggregator data>
    // <masterCompute data>
    FSDataOutputStream finalizedOutputStream =
        getFs().create(finalizedCheckpointPath);

    String superstepFinishedNode =
        getSuperstepFinishedPath(getApplicationAttempt(), superstep - 1);
    finalizedOutputStream.write(
        getZkExt().getData(superstepFinishedNode, false, null));

    finalizedOutputStream.writeInt(chosenWorkerInfoList.size());
    for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
      String chosenWorkerInfoPrefix =
          getCheckpointBasePath(superstep) + "." +
              chosenWorkerInfo.getHostnameId();
      finalizedOutputStream.writeUTF(chosenWorkerInfoPrefix);
    }
    aggregatorHandler.write(finalizedOutputStream);
    masterCompute.write(finalizedOutputStream);
    finalizedOutputStream.close();
    lastCheckpointedSuperstep = superstep;
    GiraphStats.getInstance().
        getLastCheckpointedSuperstep().setValue(superstep);
  }

  /**
   * Assign the partitions for this superstep.  If there are changes,
   * the workers will know how to do the exchange.  If this was a restarted
   * superstep, then make sure to provide information on where to find the
   * checkpoint file.
   *
   * @param allPartitionStatsList All partition stats
   * @param chosenWorkerInfoList All the chosen worker infos
   * @param masterGraphPartitioner Master graph partitioner
   */
  private void assignPartitionOwners(
      List<PartitionStats> allPartitionStatsList,
      List<WorkerInfo> chosenWorkerInfoList,
      MasterGraphPartitioner<I, V, E> masterGraphPartitioner) {
    Collection<PartitionOwner> partitionOwners;
    if (getSuperstep() == INPUT_SUPERSTEP ||
        getSuperstep() == getRestartedSuperstep()) {
      partitionOwners =
          masterGraphPartitioner.createInitialPartitionOwners(
              chosenWorkerInfoList, maxWorkers);
      if (partitionOwners.isEmpty()) {
        throw new IllegalStateException(
            "assignAndExchangePartitions: No partition owners set");
      }
    } else {
      partitionOwners =
          masterGraphPartitioner.generateChangedPartitionOwners(
              allPartitionStatsList,
              chosenWorkerInfoList,
              maxWorkers,
              getSuperstep());

      PartitionUtils.analyzePartitionStats(partitionOwners,
          allPartitionStatsList);
    }
    checkPartitions(masterGraphPartitioner.getCurrentPartitionOwners());

    // If restarted, prepare the checkpoint restart
    if (getRestartedSuperstep() == getSuperstep()) {
      try {
        prepareCheckpointRestart(getSuperstep(), partitionOwners);
      } catch (IOException e) {
        throw new IllegalStateException(
            "assignPartitionOwners: IOException on preparing", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "assignPartitionOwners: KeeperException on preparing", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "assignPartitionOwners: InteruptedException on preparing",
            e);
      }
    }

    // There will be some exchange of partitions
    if (!partitionOwners.isEmpty()) {
      String vertexExchangePath =
          getPartitionExchangePath(getApplicationAttempt(),
              getSuperstep());
      try {
        getZkExt().createOnceExt(vertexExchangePath,
            null,
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT,
            true);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "assignPartitionOwners: KeeperException creating " +
                vertexExchangePath);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "assignPartitionOwners: InterruptedException creating " +
                vertexExchangePath);
      }
    }

    // Workers are waiting for these assignments
    AddressesAndPartitionsWritable addressesAndPartitions =
        new AddressesAndPartitionsWritable(masterInfo, chosenWorkerInfoList,
            partitionOwners);
    String addressesAndPartitionsPath =
        getAddressesAndPartitionsPath(getApplicationAttempt(),
            getSuperstep());
    WritableUtils.writeToZnode(
        getZkExt(),
        addressesAndPartitionsPath,
        -1,
        addressesAndPartitions);
  }

  /**
   * Check if partition ids are valid
   *
   * @param partitionOwners List of partition ids for current superstep
   */
  private void checkPartitions(Collection<PartitionOwner> partitionOwners) {
    for (PartitionOwner partitionOwner : partitionOwners) {
      int partitionId = partitionOwner.getPartitionId();
      if (partitionId < 0 || partitionId >= partitionOwners.size()) {
        throw new IllegalStateException("checkPartitions: " +
            "Invalid partition id " + partitionId +
            " - partition ids must be values from 0 to (numPartitions - 1)");
      }
    }
  }

  /**
   * Check whether the workers chosen for this superstep are still alive
   *
   * @param chosenWorkerInfoHealthPath Path to the healthy workers in ZooKeeper
   * @param chosenWorkerInfoList List of the healthy workers
   * @return true if they are all alive, false otherwise.
   * @throws InterruptedException
   * @throws KeeperException
   */
  private boolean superstepChosenWorkerAlive(
    String chosenWorkerInfoHealthPath,
    List<WorkerInfo> chosenWorkerInfoList)
    throws KeeperException, InterruptedException {
    List<WorkerInfo> chosenWorkerInfoHealthyList =
        getWorkerInfosFromPath(chosenWorkerInfoHealthPath, false);
    Set<WorkerInfo> chosenWorkerInfoHealthySet =
        new HashSet<WorkerInfo>(chosenWorkerInfoHealthyList);
    boolean allChosenWorkersHealthy = true;
    for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
      if (!chosenWorkerInfoHealthySet.contains(chosenWorkerInfo)) {
        allChosenWorkersHealthy = false;
        LOG.error("superstepChosenWorkerAlive: Missing chosen " +
            "worker " + chosenWorkerInfo +
            " on superstep " + getSuperstep());
      }
    }
    return allChosenWorkersHealthy;
  }

  @Override
  public void restartFromCheckpoint(long checkpoint) {
    // Process:
    // 1. Remove all old input split data
    // 2. Increase the application attempt and set to the correct checkpoint
    // 3. Send command to all workers to restart their tasks
    try {
      getZkExt().deleteExt(vertexInputSplitsPaths.getPath(), -1,
          true);
      getZkExt().deleteExt(edgeInputSplitsPaths.getPath(), -1,
          true);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "restartFromCheckpoint: InterruptedException", e);
    } catch (KeeperException e) {
      throw new RuntimeException(
          "restartFromCheckpoint: KeeperException", e);
    }
    setApplicationAttempt(getApplicationAttempt() + 1);
    setCachedSuperstep(checkpoint);
    setRestartedSuperstep(checkpoint);
    setJobState(ApplicationState.START_SUPERSTEP,
        getApplicationAttempt(),
        checkpoint);
  }

  /**
   * Only get the finalized checkpoint files
   */
  public static class FinalizedCheckpointPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(BspService.CHECKPOINT_FINALIZED_POSTFIX);
    }
  }

  @Override
  public long getLastGoodCheckpoint() throws IOException {
    // Find the last good checkpoint if none have been written to the
    // knowledge of this master
    if (lastCheckpointedSuperstep == -1) {
      try {
        FileStatus[] fileStatusArray =
            getFs().listStatus(new Path(checkpointBasePath),
                new FinalizedCheckpointPathFilter());
        if (fileStatusArray == null) {
          return -1;
        }
        Arrays.sort(fileStatusArray);
        lastCheckpointedSuperstep = getCheckpoint(
            fileStatusArray[fileStatusArray.length - 1].getPath());
        if (LOG.isInfoEnabled()) {
          LOG.info("getLastGoodCheckpoint: Found last good checkpoint " +
              lastCheckpointedSuperstep + " from " +
              fileStatusArray[fileStatusArray.length - 1].
                  getPath().toString());
        }
      } catch (IOException e) {
        LOG.fatal("getLastGoodCheckpoint: No last good checkpoints can be " +
            "found, killing the job.", e);
        failJob(e);
      }
    }

    return lastCheckpointedSuperstep;
  }

  /**
   * Wait for a set of workers to signal that they are done with the
   * barrier.
   *
   * @param finishedWorkerPath Path to where the workers will register their
   *        hostname and id
   * @param workerInfoList List of the workers to wait for
   * @param event Event to wait on for a chance to be done.
   * @return True if barrier was successful, false if there was a worker
   *         failure
   */
  private boolean barrierOnWorkerList(String finishedWorkerPath,
      List<WorkerInfo> workerInfoList,
      BspEvent event) {
    try {
      getZkExt().createOnceExt(finishedWorkerPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "barrierOnWorkerList: KeeperException - Couldn't create " +
              finishedWorkerPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "barrierOnWorkerList: InterruptedException - Couldn't create " +
              finishedWorkerPath, e);
    }
    List<String> hostnameIdList =
        new ArrayList<String>(workerInfoList.size());
    for (WorkerInfo workerInfo : workerInfoList) {
      hostnameIdList.add(workerInfo.getHostnameId());
    }
    String workerInfoHealthyPath =
        getWorkerInfoHealthyPath(getApplicationAttempt(), getSuperstep());
    List<String> finishedHostnameIdList;
    long nextInfoMillis = System.currentTimeMillis();
    final int defaultTaskTimeoutMsec = 10 * 60 * 1000;  // from TaskTracker
    final int taskTimeoutMsec = getContext().getConfiguration().getInt(
        "mapred.task.timeout", defaultTaskTimeoutMsec);
    while (true) {
      try {
        finishedHostnameIdList =
            getZkExt().getChildrenExt(finishedWorkerPath,
                true,
                false,
                false);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "barrierOnWorkerList: KeeperException - Couldn't get " +
                "children of " + finishedWorkerPath, e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "barrierOnWorkerList: IllegalException - Couldn't get " +
                "children of " + finishedWorkerPath, e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("barrierOnWorkerList: Got finished worker list = " +
            finishedHostnameIdList + ", size = " +
            finishedHostnameIdList.size() +
            ", worker list = " +
            workerInfoList + ", size = " +
            workerInfoList.size() +
            " from " + finishedWorkerPath);
      }

      if (LOG.isInfoEnabled() &&
          (System.currentTimeMillis() > nextInfoMillis)) {
        nextInfoMillis = System.currentTimeMillis() + 30000;
        LOG.info("barrierOnWorkerList: " +
            finishedHostnameIdList.size() +
            " out of " + workerInfoList.size() +
            " workers finished on superstep " +
            getSuperstep() + " on path " + finishedWorkerPath);
        if (workerInfoList.size() - finishedHostnameIdList.size() <
            MAX_PRINTABLE_REMAINING_WORKERS) {
          Set<String> remainingWorkers = Sets.newHashSet(hostnameIdList);
          remainingWorkers.removeAll(finishedHostnameIdList);
          LOG.info("barrierOnWorkerList: Waiting on " + remainingWorkers);
        }
      }
      getContext().setStatus(getGraphTaskManager().getGraphFunctions() + " - " +
          finishedHostnameIdList.size() +
          " finished out of " +
          workerInfoList.size() +
          " on superstep " + getSuperstep());
      if (finishedHostnameIdList.containsAll(hostnameIdList)) {
        break;
      }

      // Wait for a signal or timeout
      event.waitMsecs(taskTimeoutMsec / 2);
      event.reset();
      getContext().progress();

      // Did a worker die?
      try {
        if ((getSuperstep() > 0) &&
            !superstepChosenWorkerAlive(
                workerInfoHealthyPath,
                workerInfoList)) {
          return false;
        }
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "barrierOnWorkerList: KeeperException - " +
                "Couldn't get " + workerInfoHealthyPath, e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "barrierOnWorkerList: InterruptedException - " +
                "Couldn't get " + workerInfoHealthyPath, e);
      }
    }

    return true;
  }

  /**
   * Clean up old superstep data from Zookeeper
   *
   * @param removeableSuperstep Supersteo to clean up
   * @throws InterruptedException
   */
  private void cleanUpOldSuperstep(long removeableSuperstep) throws
      InterruptedException {
    if (KEEP_ZOOKEEPER_DATA.isFalse(getConfiguration()) &&
        (removeableSuperstep >= 0)) {
      String oldSuperstepPath =
          getSuperstepPath(getApplicationAttempt()) + "/" +
              removeableSuperstep;
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("coordinateSuperstep: Cleaning up old Superstep " +
              oldSuperstepPath);
        }
        getZkExt().deleteExt(oldSuperstepPath,
            -1,
            true);
      } catch (KeeperException.NoNodeException e) {
        LOG.warn("coordinateBarrier: Already cleaned up " +
            oldSuperstepPath);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "coordinateSuperstep: KeeperException on " +
                "finalizing checkpoint", e);
      }
    }
  }

  /**
   * Coordinate the exchange of vertex/edge input splits among workers.
   *
   * @param inputSplitPaths Input split paths
   * @param inputSplitEvents Input split events
   * @param inputSplitsType Type of input splits (for logging purposes)
   */
  private void coordinateInputSplits(InputSplitPaths inputSplitPaths,
                                     InputSplitEvents inputSplitEvents,
                                     String inputSplitsType) {
    // Coordinate the workers finishing sending their vertices/edges to the
    // correct workers and signal when everything is done.
    String logPrefix = "coordinate" + inputSplitsType + "InputSplits";
    if (!barrierOnWorkerList(inputSplitPaths.getDonePath(),
        chosenWorkerInfoList,
        inputSplitEvents.getDoneStateChanged())) {
      throw new IllegalStateException(logPrefix + ": Worker failed during " +
          "input split (currently not supported)");
    }
    try {
      getZkExt().createExt(inputSplitPaths.getAllDonePath(),
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          false);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("coordinateInputSplits: Node " +
          inputSplitPaths.getAllDonePath() + " already exists.");
    } catch (KeeperException e) {
      throw new IllegalStateException(logPrefix + ": KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(logPrefix + ": IllegalStateException", e);
    }
  }

  /**
   * Initialize aggregator at the master side
   * before vertex/edge loading.
   * This methods cooperates with other code
   * to enables aggregation usage at INPUT_SUPERSTEP
   * Other codes are:
   *  BSPServiceWorker:
   *  aggregatorHandler.prepareSuperstep in
   *  setup
   *  set aggregator usage in vertexReader and
   *  edgeReader
   *
   * @throws InterruptedException
   */
  private void initializeAggregatorInputSuperstep()
    throws InterruptedException {
    aggregatorHandler.prepareSuperstep(masterClient);
    prepareMasterCompute(getSuperstep());
    try {
      masterCompute.initialize();
    } catch (InstantiationException e) {
      LOG.fatal(
        "initializeAggregatorInputSuperstep: Failed in instantiation", e);
      throw new RuntimeException(
        "initializeAggregatorInputSuperstep: Failed in instantiation", e);
    } catch (IllegalAccessException e) {
      LOG.fatal("initializeAggregatorInputSuperstep: Failed in access", e);
      throw new RuntimeException(
        "initializeAggregatorInputSuperstep: Failed in access", e);
    }
    aggregatorHandler.finishSuperstep(masterClient);
  }

  /**
   * This is required before initialization
   * and run of MasterCompute
   *
   * @param superstep superstep for which to run masterCompute
   * @return Superstep classes set by masterCompute
   */
  private SuperstepClasses prepareMasterCompute(long superstep) {
    GraphState graphState = new GraphState(superstep ,
        GiraphStats.getInstance().getVertices().getValue(),
        GiraphStats.getInstance().getEdges().getValue(),
        getContext());
    SuperstepClasses superstepClasses =
      new SuperstepClasses(getConfiguration());
    masterCompute.setGraphState(graphState);
    masterCompute.setSuperstepClasses(superstepClasses);
    return superstepClasses;
  }

  @Override
  public SuperstepState coordinateSuperstep() throws
  KeeperException, InterruptedException {
    // 1. Get chosen workers and set up watches on them.
    // 2. Assign partitions to the workers
    //    (possibly reloading from a superstep)
    // 3. Wait for all workers to complete
    // 4. Collect and process aggregators
    // 5. Create superstep finished node
    // 6. If the checkpoint frequency is met, finalize the checkpoint

    for (MasterObserver observer : observers) {
      observer.preSuperstep(getSuperstep());
      getContext().progress();
    }

    chosenWorkerInfoList = checkWorkers();
    if (chosenWorkerInfoList == null) {
      setJobStateFailed("coordinateSuperstep: Not enough healthy workers for " +
                    "superstep " + getSuperstep());
    } else {
      for (WorkerInfo workerInfo : chosenWorkerInfoList) {
        String workerInfoHealthyPath =
            getWorkerInfoHealthyPath(getApplicationAttempt(),
                getSuperstep()) + "/" +
                workerInfo.getHostnameId();
        if (getZkExt().exists(workerInfoHealthyPath, true) == null) {
          LOG.warn("coordinateSuperstep: Chosen worker " +
              workerInfoHealthyPath +
              " is no longer valid, failing superstep");
        }
      }
    }

    masterClient.openConnections();

    GiraphStats.getInstance().
        getCurrentWorkers().setValue(chosenWorkerInfoList.size());
    assignPartitionOwners(allPartitionStatsList,
        chosenWorkerInfoList,
        masterGraphPartitioner);

    // We need to finalize aggregators from previous superstep (send them to
    // worker owners) after new worker assignments
    if (getSuperstep() >= 0) {
      aggregatorHandler.finishSuperstep(masterClient);
    }

    // Finalize the valid checkpoint file prefixes and possibly
    // the aggregators.
    if (checkpointFrequencyMet(getSuperstep())) {
      String workerWroteCheckpointPath =
          getWorkerWroteCheckpointPath(getApplicationAttempt(),
              getSuperstep());
      // first wait for all the workers to write their checkpoint data
      if (!barrierOnWorkerList(workerWroteCheckpointPath,
          chosenWorkerInfoList,
          getWorkerWroteCheckpointEvent())) {
        return SuperstepState.WORKER_FAILURE;
      }
      try {
        finalizeCheckpoint(getSuperstep(), chosenWorkerInfoList);
      } catch (IOException e) {
        throw new IllegalStateException(
            "coordinateSuperstep: IOException on finalizing checkpoint",
            e);
      }
    }

    if (getSuperstep() == INPUT_SUPERSTEP) {
      // Initialize aggregators before coordinating
      // vertex loading and edge loading
      initializeAggregatorInputSuperstep();
      if (getConfiguration().hasVertexInputFormat()) {
        coordinateInputSplits(vertexInputSplitsPaths, vertexInputSplitsEvents,
            "Vertex");
      }
      if (getConfiguration().hasEdgeInputFormat()) {
        coordinateInputSplits(edgeInputSplitsPaths, edgeInputSplitsEvents,
            "Edge");
      }
    }

    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep());
    if (!barrierOnWorkerList(finishedWorkerPath,
        chosenWorkerInfoList,
        getSuperstepStateChangedEvent())) {
      return SuperstepState.WORKER_FAILURE;
    }

    // Collect aggregator values, then run the master.compute() and
    // finally save the aggregator values
    aggregatorHandler.prepareSuperstep(masterClient);
    SuperstepClasses superstepClasses =
      prepareMasterCompute(getSuperstep() + 1);
    doMasterCompute();

    // If the master is halted or all the vertices voted to halt and there
    // are no more messages in the system, stop the computation
    GlobalStats globalStats = aggregateWorkerStats(getSuperstep());
    if (masterCompute.isHalted() ||
        (globalStats.getFinishedVertexCount() ==
        globalStats.getVertexCount() &&
        globalStats.getMessageCount() == 0)) {
      globalStats.setHaltComputation(true);
    } else if (getZkExt().exists(haltComputationPath, false) != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Halting computation because halt zookeeper node was created");
      }
      globalStats.setHaltComputation(true);
    }

    // If we have completed the maximum number of supersteps, stop
    // the computation
    if (maxNumberOfSupersteps !=
        GiraphConstants.MAX_NUMBER_OF_SUPERSTEPS.getDefaultValue() &&
        (getSuperstep() == maxNumberOfSupersteps - 1)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("coordinateSuperstep: Finished " + maxNumberOfSupersteps +
            " supersteps (max specified by the user), halting");
      }
      globalStats.setHaltComputation(true);
    }

    // Superstep 0 doesn't need to have matching types (Message types may not
    // match) and if the computation is halted, no need to check any of
    // the types.
    if (!globalStats.getHaltComputation()) {
      superstepClasses.verifyTypesMatch(
          getConfiguration(), getSuperstep() != 0);
    }
    getConfiguration().updateSuperstepClasses(superstepClasses);

    // Let everyone know the aggregated application state through the
    // superstep finishing znode.
    String superstepFinishedNode =
        getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
    WritableUtils.writeToZnode(
        getZkExt(), superstepFinishedNode, -1, globalStats, superstepClasses);
    updateCounters(globalStats);

    cleanUpOldSuperstep(getSuperstep() - 1);
    incrCachedSuperstep();
    // Counter starts at zero, so no need to increment
    if (getSuperstep() > 0) {
      GiraphStats.getInstance().getSuperstepCounter().increment();
    }
    SuperstepState superstepState;
    if (globalStats.getHaltComputation()) {
      superstepState = SuperstepState.ALL_SUPERSTEPS_DONE;
    } else {
      superstepState = SuperstepState.THIS_SUPERSTEP_DONE;
    }
    aggregatorHandler.writeAggregators(getSuperstep(), superstepState);

    return superstepState;
  }

  /**
   * This doMasterCompute is only called
   * after masterCompute is initialized
   */
  private void doMasterCompute() {
    GiraphTimerContext timerContext = masterComputeTimer.time();
    masterCompute.compute();
    timerContext.stop();
  }

  /**
   * Need to clean up ZooKeeper nicely.  Make sure all the masters and workers
   * have reported ending their ZooKeeper connections.
   */
  private void cleanUpZooKeeper() {
    try {
      getZkExt().createExt(cleanedUpPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("cleanUpZooKeeper: Node " + cleanedUpPath +
            " already exists, no need to create.");
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "cleanupZooKeeper: Got KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "cleanupZooKeeper: Got IllegalStateException", e);
    }
    // Need to wait for the number of workers and masters to complete
    int maxTasks = BspInputFormat.getMaxTasks(getConfiguration());
    if ((getGraphTaskManager().getGraphFunctions() == GraphFunctions.ALL) ||
        (getGraphTaskManager().getGraphFunctions() ==
        GraphFunctions.ALL_EXCEPT_ZOOKEEPER)) {
      maxTasks *= 2;
    }
    List<String> cleanedUpChildrenList = null;
    while (true) {
      try {
        cleanedUpChildrenList =
            getZkExt().getChildrenExt(
                cleanedUpPath, true, false, true);
        if (LOG.isInfoEnabled()) {
          LOG.info("cleanUpZooKeeper: Got " +
              cleanedUpChildrenList.size() + " of " +
              maxTasks  +  " desired children from " +
              cleanedUpPath);
        }
        if (cleanedUpChildrenList.size() == maxTasks) {
          break;
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("cleanedUpZooKeeper: Waiting for the " +
              "children of " + cleanedUpPath +
              " to change since only got " +
              cleanedUpChildrenList.size() + " nodes.");
        }
      } catch (KeeperException e) {
        // We are in the cleanup phase -- just log the error
        LOG.error("cleanUpZooKeeper: Got KeeperException, " +
            "but will continue", e);
        return;
      } catch (InterruptedException e) {
        // We are in the cleanup phase -- just log the error
        LOG.error("cleanUpZooKeeper: Got InterruptedException, " +
            "but will continue", e);
        return;
      }

      getCleanedUpChildrenChangedEvent().waitForever();
      getCleanedUpChildrenChangedEvent().reset();
    }

    // At this point, all processes have acknowledged the cleanup,
    // and the master can do any final cleanup if the ZooKeeper service was
    // provided (not dynamically started) and we don't want to keep the data
    try {
      if (getConfiguration().isZookeeperExternal() &&
          KEEP_ZOOKEEPER_DATA.isFalse(getConfiguration())) {
        if (LOG.isInfoEnabled()) {
          LOG.info("cleanupZooKeeper: Removing the following path " +
              "and all children - " + basePath + " from ZooKeeper list " +
              getConfiguration().getZookeeperList());
        }
        getZkExt().deleteExt(basePath, -1, true);
      }
    } catch (KeeperException e) {
      LOG.error("cleanupZooKeeper: Failed to do cleanup of " +
          basePath + " due to KeeperException", e);
    } catch (InterruptedException e) {
      LOG.error("cleanupZooKeeper: Failed to do cleanup of " +
          basePath + " due to InterruptedException", e);
    }
  }

  @Override
  public void postApplication() {
    for (MasterObserver observer : observers) {
      observer.postApplication();
      getContext().progress();
    }
  }

  @Override
  public void postSuperstep() {
    for (MasterObserver observer : observers) {
      observer.postSuperstep(getSuperstep());
      getContext().progress();
    }
  }

  @Override
  public void failureCleanup(Exception e) {
    for (MasterObserver observer : observers) {
      try {
        observer.applicationFailed(e);
        // CHECKSTYLE: stop IllegalCatchCheck
      } catch (RuntimeException re) {
        // CHECKSTYLE: resume IllegalCatchCheck
        LOG.error(re.getClass().getName() + " from observer " +
            observer.getClass().getName(), re);
      }
      getContext().progress();
    }
  }

  @Override
  public void cleanup() throws IOException {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();

    // All master processes should denote they are done by adding special
    // znode.  Once the number of znodes equals the number of partitions
    // for workers and masters, the master will clean up the ZooKeeper
    // znodes associated with this job.
    String masterCleanedUpPath = cleanedUpPath  + "/" +
        getTaskPartition() + MASTER_SUFFIX;
    try {
      String finalFinishedPath =
          getZkExt().createExt(masterCleanedUpPath,
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
            masterCleanedUpPath);
      }
    } catch (KeeperException e) {
      LOG.error("cleanup: Got KeeperException, continuing", e);
    } catch (InterruptedException e) {
      LOG.error("cleanup: Got InterruptedException, continuing", e);
    }

    if (isMaster) {
      getGraphTaskManager().setIsMaster(true);
      cleanUpZooKeeper();
      // If desired, cleanup the checkpoint directory
      if (GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS.get(conf)) {
        boolean success =
            getFs().delete(new Path(checkpointBasePath), true);
        if (LOG.isInfoEnabled()) {
          LOG.info("cleanup: Removed HDFS checkpoint directory (" +
              checkpointBasePath + ") with return = " +
              success + " since the job " + getContext().getJobName() +
              " succeeded ");
        }
      }
      aggregatorHandler.close();
      masterClient.closeConnections();
      masterServer.close();
    }

    try {
      getZkExt().close();
    } catch (InterruptedException e) {
      // cleanup phase -- just log the error
      LOG.error("cleanup: Zookeeper failed to close", e);
    }
  }

  /**
   * Event that the master watches that denotes when a worker wrote checkpoint
   *
   * @return Event that denotes when a worker wrote checkpoint
   */
  public final BspEvent getWorkerWroteCheckpointEvent() {
    return workerWroteCheckpoint;
  }

  /**
   * Event that the master watches that denotes if a worker has done something
   * that changes the state of a superstep (either a worker completed or died)
   *
   * @return Event that denotes a superstep state change
   */
  public final BspEvent getSuperstepStateChangedEvent() {
    return superstepStateChanged;
  }

  /**
   * Should this worker failure cause the current superstep to fail?
   *
   * @param failedWorkerPath Full path to the failed worker
   */
  private void checkHealthyWorkerFailure(String failedWorkerPath) {
    if (getSuperstepFromPath(failedWorkerPath) < getSuperstep()) {
      return;
    }

    Collection<PartitionOwner> partitionOwners =
        masterGraphPartitioner.getCurrentPartitionOwners();
    String hostnameId =
        getHealthyHostnameIdFromPath(failedWorkerPath);
    for (PartitionOwner partitionOwner : partitionOwners) {
      WorkerInfo workerInfo = partitionOwner.getWorkerInfo();
      WorkerInfo previousWorkerInfo =
          partitionOwner.getPreviousWorkerInfo();
      if (workerInfo.getHostnameId().equals(hostnameId) ||
          ((previousWorkerInfo != null) &&
              previousWorkerInfo.getHostnameId().equals(hostnameId))) {
        LOG.warn("checkHealthyWorkerFailure: " +
            "at least one healthy worker went down " +
            "for superstep " + getSuperstep() + " - " +
            hostnameId + ", will try to restart from " +
            "checkpointed superstep " +
            lastCheckpointedSuperstep);
        superstepStateChanged.signal();
      }
    }
  }

  @Override
  public boolean processEvent(WatchedEvent event) {
    boolean foundEvent = false;
    if (event.getPath().contains(WORKER_HEALTHY_DIR) &&
        (event.getType() == EventType.NodeDeleted)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("processEvent: Healthy worker died (node deleted) " +
            "in " + event.getPath());
      }
      checkHealthyWorkerFailure(event.getPath());
      superstepStateChanged.signal();
      foundEvent = true;
    } else if (event.getPath().contains(WORKER_FINISHED_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("processEvent: Worker finished (node change) " +
            "event - superstepStateChanged signaled");
      }
      superstepStateChanged.signal();
      foundEvent = true;
    } else if (event.getPath().contains(WORKER_WROTE_CHECKPOINT_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("processEvent: Worker wrote checkpoint (node change) " +
            "event - workerWroteCheckpoint signaled");
      }
      workerWroteCheckpoint.signal();
      foundEvent = true;
    }

    return foundEvent;
  }

  /**
   * Set values of counters to match the ones from {@link GlobalStats}
   *
   * @param globalStats Global statistics which holds new counter values
   */
  private void updateCounters(GlobalStats globalStats) {
    GiraphStats gs = GiraphStats.getInstance();
    gs.getVertices().setValue(globalStats.getVertexCount());
    gs.getFinishedVertexes().setValue(globalStats.getFinishedVertexCount());
    gs.getEdges().setValue(globalStats.getEdgeCount());
    gs.getSentMessages().setValue(globalStats.getMessageCount());
    gs.getSentMessageBytes().setValue(globalStats.getMessageBytesCount());
  }

  /**
   * Task that writes a given input split to zookeeper.
   * Upon failure call() throws an exception.
   */
  private class WriteInputSplit implements Callable<Void> {
    /** Input format */
    private final GiraphInputFormat inputFormat;
    /** Input split which we are going to write */
    private final InputSplit inputSplit;
    /** Input splits path */
    private final String inputSplitsPath;
    /** Index of the input split */
    private final int index;
    /** Whether to write locality information */
    private final boolean writeLocations;

    /**
     * Constructor
     *
     * @param inputFormat Input format
     * @param inputSplit Input split which we are going to write
     * @param inputSplitsPath Input splits path
     * @param index Index of the input split
     * @param writeLocations whether to write the input split's locations (to
     *                       be used by workers for prioritizing local splits
     *                       when reading)
     */
    public WriteInputSplit(GiraphInputFormat inputFormat,
                           InputSplit inputSplit,
                           String inputSplitsPath,
                           int index,
                           boolean writeLocations) {
      this.inputFormat = inputFormat;
      this.inputSplit = inputSplit;
      this.inputSplitsPath = inputSplitsPath;
      this.index = index;
      this.writeLocations = writeLocations;
    }

    @Override
    public Void call() {
      String inputSplitPath = null;
      try {
        ByteArrayOutputStream byteArrayOutputStream =
            new ByteArrayOutputStream();
        DataOutput outputStream =
            new DataOutputStream(byteArrayOutputStream);

        if (writeLocations) {
          String[] splitLocations = inputSplit.getLocations();
          StringBuilder locations = null;
          if (splitLocations != null) {
            int splitListLength =
                Math.min(splitLocations.length, localityLimit);
            locations = new StringBuilder();
            for (String location : splitLocations) {
              locations.append(location)
                  .append(--splitListLength > 0 ? "\t" : "");
            }
          }
          Text.writeString(outputStream,
              locations == null ? "" : locations.toString());
        }

        inputFormat.writeInputSplit(inputSplit, outputStream);
        inputSplitPath = inputSplitsPath + "/" + index;
        getZkExt().createExt(inputSplitPath,
            byteArrayOutputStream.toByteArray(),
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT,
            true);

        if (LOG.isDebugEnabled()) {
          LOG.debug("call: Created input split " +
              "with index " + index + " serialized as " +
              byteArrayOutputStream.toString(Charset.defaultCharset().name()));
        }
      } catch (KeeperException.NodeExistsException e) {
        if (LOG.isInfoEnabled()) {
          LOG.info("call: Node " +
              inputSplitPath + " already exists.");
        }
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "call: KeeperException", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "call: IllegalStateException", e);
      } catch (IOException e) {
        throw new IllegalStateException(
            "call: IOException", e);
      }
      return null;
    }
  }
}
