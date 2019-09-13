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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.iharder.Base64;
import org.apache.commons.io.FilenameUtils;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.bsp.checkpoints.CheckpointSupportedChecker;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.MasterServer;
import org.apache.giraph.comm.netty.NettyMasterClient;
import org.apache.giraph.comm.netty.NettyMasterServer;
import org.apache.giraph.comm.requests.AddressesAndPartitionsRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.graph.AddressesAndPartitionsWritable;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.GraphFunctions;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.master.input.MasterInputSplitsHandler;
import org.apache.giraph.metrics.AggregatedMetrics;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphTimer;
import org.apache.giraph.metrics.GiraphTimerContext;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.metrics.WorkerSuperstepMetrics;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionUtils;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.giraph.utils.JMapHistoDumper;
import org.apache.giraph.utils.ReactiveJMapHistoDumper;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.apache.giraph.conf.GiraphConstants.INPUT_SPLIT_SAMPLE_PERCENT;
import static org.apache.giraph.conf.GiraphConstants.KEEP_ZOOKEEPER_DATA;
import static org.apache.giraph.conf.GiraphConstants.PARTITION_LONG_TAIL_MIN_PRINT;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceMaster}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes, unchecked")
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
  /** Handler for global communication */
  private MasterGlobalCommHandler globalCommHandler;
  /** Handler for aggregators to reduce/broadcast translation */
  private AggregatorToGlobalCommTranslation aggregatorTranslation;
  /** Master class */
  private MasterCompute masterCompute;
  /** IPC Client */
  private MasterClient masterClient;
  /** IPC Server */
  private MasterServer masterServer;
  /** Master info */
  private MasterInfo masterInfo;
  /** List of workers in current superstep, sorted by task id */
  private List<WorkerInfo> chosenWorkerInfoList = Lists.newArrayList();
  /** Observers over master lifecycle. */
  private final MasterObserver[] observers;

  // Per-Superstep Metrics
  /** MasterCompute time */
  private GiraphTimer masterComputeTimer;

  /** Checkpoint frequency */
  private final int checkpointFrequency;
  /** Current checkpoint status */
  private CheckpointStatus checkpointStatus;
  /** Checks if checkpointing supported */
  private final CheckpointSupportedChecker checkpointSupportedChecker;

  /**
   * Constructor for setting up the master.
   *
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   */
  public BspServiceMaster(
      Mapper<?, ?, ?, ?>.Context context,
      GraphTaskManager<I, V, E> graphTaskManager) {
    super(context, graphTaskManager);
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
    if (conf.isReactiveJmapHistogramDumpEnabled()) {
      conf.addMasterObserverClass(ReactiveJMapHistoDumper.class);
    }
    observers = conf.createMasterObservers(context);

    this.checkpointFrequency = conf.getCheckpointFrequency();
    this.checkpointStatus = CheckpointStatus.NONE;
    this.checkpointSupportedChecker =
        ReflectionUtils.newInstance(
            GiraphConstants.CHECKPOINT_SUPPORTED_CHECKER.get(conf));

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
      LOG.info("setJobState: " + jobState);
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
    getGraphTaskManager().getJobProgressTracker().logFailure(reason);
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
                                               InputType inputSplitType) {
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
      Collections.shuffle(splits);
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
        try {
          @SuppressWarnings("deprecation")
          JobID jobId = JobID.forName(getJobId());
          RunningJob job = jobClient.getJob(jobId);
          if (job != null) {
            job.killJob();
          } else {
            LOG.error("Job not found for jobId=" + getJobId());
          }
        } catch (IllegalArgumentException iae) {
          LOG.info("This job (" + getJobId() +
                       ") is not a legacy Hadoop job and will " +
                       "continue with failure cleanup." +
                       e.getMessage(),
                   e);
        }
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
      try {
        WritableUtils.readFieldsFromZnode(
            getZkExt(), workerInfoPath, true, null, workerInfo);
        workerInfoList.add(workerInfo);
      } catch (IllegalStateException e) {
        LOG.warn("Can't get info from worker, did it die in between? " +
            "workerInfoPath=" + workerInfoPath, e);
      }
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

  @Override
  public List<WorkerInfo> checkWorkers() {
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
        partitionSet.add(workerInfo.getTaskId() % maxWorkers);
      }
      for (WorkerInfo workerInfo : unhealthyWorkerInfoList) {
        partitionSet.add(workerInfo.getTaskId() % maxWorkers);
      }
      for (int i = 1; i <= maxWorkers; ++i) {
        if (partitionSet.contains(Integer.valueOf(i))) {
          continue;
        } else if (i == getTaskId() % maxWorkers) {
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
   * @param inputSplitType Type of input split (for logging purposes)
   * @return Number of splits. Returns -1 on failure to create
   *         valid input splits.
   */
  private int createInputSplits(GiraphInputFormat inputFormat,
                                InputType inputSplitType) {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();
    String logPrefix = "create" + inputSplitType + "InputSplits";
    // Only the 'master' should be doing this.  Wait until the number of
    // processes that have reported health exceeds the minimum percentage.
    // If the minimum percentage is not met, fail the job.  Otherwise
    // generate the input splits
    List<WorkerInfo> healthyWorkerInfoList = checkWorkers();
    if (healthyWorkerInfoList == null) {
      setJobStateFailed("Not enough healthy workers to create input splits");
      return -1;
    }
    globalCommHandler.getInputSplitsHandler().initialize(masterClient,
        healthyWorkerInfoList);

    // Create at least as many splits as the total number of input threads.
    int minSplitCountHint = healthyWorkerInfoList.size() *
        conf.getNumInputSplitsThreads();

    // Note that the input splits may only be a sample if
    // INPUT_SPLIT_SAMPLE_PERCENT is set to something other than 100
    List<InputSplit> splitList = generateInputSplits(inputFormat,
        minSplitCountHint, inputSplitType);

    if (splitList.isEmpty() && GiraphConstants.FAIL_ON_EMPTY_INPUT.get(conf)) {
      LOG.fatal(logPrefix + ": Failing job due to 0 input splits, " +
          "check input of " + inputFormat.getClass().getName() + "!");
      getContext().setStatus("Failing job due to 0 input splits, " +
          "check input of " + inputFormat.getClass().getName() + "!");
      setJobStateFailed("******* PLEASE CHECK YOUR INPUT TABLES - PARTITIONS " +
          "WHICH YOU SPECIFIED ARE MISSING (for " + inputSplitType +
          " input). FAILING THE JOB *******");
    }
    if (minSplitCountHint > splitList.size()) {
      LOG.warn(logPrefix + ": Number of inputSplits=" +
          splitList.size() + " < " +
          minSplitCountHint +
          "=total number of input threads, " +
          "some threads will be not used");
    }

    globalCommHandler.getInputSplitsHandler().addSplits(inputSplitType,
        splitList, inputFormat);

    return splitList.size();
  }

  @Override
  public int createMappingInputSplits() {
    if (!getConfiguration().hasMappingInputFormat()) {
      return 0;
    }
    MappingInputFormat<I, V, E, ? extends Writable> mappingInputFormat =
      getConfiguration().createWrappedMappingInputFormat();
    return createInputSplits(mappingInputFormat, InputType.MAPPING);
  }

  @Override
  public int createVertexInputSplits() {
    int splits = 0;
    if (getConfiguration().hasVertexInputFormat()) {
      VertexInputFormat<I, V, E> vertexInputFormat =
          getConfiguration().createWrappedVertexInputFormat();
      splits = createInputSplits(vertexInputFormat, InputType.VERTEX);
    }
    MasterProgress.get().setVertexInputSplitCount(splits);
    getJobProgressTracker().updateMasterProgress(MasterProgress.get());
    return splits;
  }

  @Override
  public int createEdgeInputSplits() {
    int splits = 0;
    if (getConfiguration().hasEdgeInputFormat()) {
      EdgeInputFormat<I, E> edgeInputFormat =
          getConfiguration().createWrappedEdgeInputFormat();
      splits = createInputSplits(edgeInputFormat, InputType.EDGE);
    }
    MasterProgress.get().setEdgeInputSplitsCount(splits);
    getJobProgressTracker().updateMasterProgress(MasterProgress.get());
    return splits;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return chosenWorkerInfoList;
  }

  @Override
  public MasterGlobalCommHandler getGlobalCommHandler() {
    return globalCommHandler;
  }

  @Override
  public AggregatorToGlobalCommTranslation getAggregatorTranslationHandler() {
    return aggregatorTranslation;
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
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @return Collection of generated partition owners.
   */
  private Collection<PartitionOwner> prepareCheckpointRestart(long superstep)
    throws IOException, KeeperException, InterruptedException {
    List<PartitionOwner> partitionOwners = new ArrayList<>();
    FileSystem fs = getFs();
    String finalizedCheckpointPath = getSavedCheckpointBasePath(superstep) +
        CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX;
    LOG.info("Loading checkpoint from " + finalizedCheckpointPath);
    DataInputStream finalizedStream =
        fs.open(new Path(finalizedCheckpointPath));
    GlobalStats globalStats = new GlobalStats();
    globalStats.readFields(finalizedStream);
    updateCounters(globalStats);
    SuperstepClasses superstepClasses =
        SuperstepClasses.createToRead(getConfiguration());
    superstepClasses.readFields(finalizedStream);
    getConfiguration().updateSuperstepClasses(superstepClasses);
    int prefixFileCount = finalizedStream.readInt();

    String checkpointFile =
        finalizedStream.readUTF();
    for (int i = 0; i < prefixFileCount; ++i) {
      int mrTaskId = finalizedStream.readInt();

      DataInputStream metadataStream = fs.open(new Path(checkpointFile +
          "." + mrTaskId + CheckpointingUtils.CHECKPOINT_METADATA_POSTFIX));
      long partitions = metadataStream.readInt();
      WorkerInfo worker = getWorkerInfoById(mrTaskId);
      for (long p = 0; p < partitions; ++p) {
        int partitionId = metadataStream.readInt();
        PartitionOwner partitionOwner = new BasicPartitionOwner(partitionId,
            worker);
        partitionOwners.add(partitionOwner);
        LOG.info("prepareCheckpointRestart partitionId=" + partitionId +
            " assigned to " + partitionOwner);
      }
      metadataStream.close();
    }
    //Ordering appears to be important as of right now we rely on this ordering
    //in WorkerGraphPartitioner
    Collections.sort(partitionOwners, new Comparator<PartitionOwner>() {
      @Override
      public int compare(PartitionOwner p1, PartitionOwner p2) {
        return Integer.compare(p1.getPartitionId(), p2.getPartitionId());
      }
    });


    globalCommHandler.getAggregatorHandler().readFields(finalizedStream);
    aggregatorTranslation.readFields(finalizedStream);
    masterCompute.readFields(finalizedStream);
    finalizedStream.close();

    return partitionOwners;
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
              "/" + getHostnameTaskId(),
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
              setValue(getTaskId());

          globalCommHandler = new MasterGlobalCommHandler(
              new MasterAggregatorHandler(getConfiguration(), getContext()),
              new MasterInputSplitsHandler(
                  getConfiguration().useInputSplitLocality(), getContext()));
          aggregatorTranslation = new AggregatorToGlobalCommTranslation(
              getConfiguration(), globalCommHandler);

          globalCommHandler.getAggregatorHandler().initialize(this);
          masterCompute = getConfiguration().createMasterCompute();
          masterCompute.setMasterService(this);

          masterInfo = new MasterInfo();
          masterServer =
              new NettyMasterServer(getConfiguration(), this, getContext(),
                  getGraphTaskManager().createUncaughtExceptionHandler());
          masterInfo.setInetSocketAddress(masterServer.getMyAddress(),
              masterServer.getLocalHostOrIp());
          masterInfo.setTaskId(getTaskId());
          masterClient =
              new NettyMasterClient(getContext(), getConfiguration(), this,
                  getGraphTaskManager().createUncaughtExceptionHandler());
          masterServer.setFlowControl(masterClient.getFlowControl());

          if (LOG.isInfoEnabled()) {
            LOG.info("becomeMaster: I am now the master!");
          }
          isMaster = true;
          return isMaster;
        }
        LOG.info("becomeMaster: Waiting to become the master...");
        getMasterElectionChildrenChangedEvent().waitForTimeoutOrFail(
            GiraphConstants.WAIT_ZOOKEEPER_TIMEOUT_MSEC.get(
                getConfiguration()));
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

    for (String finishedPath : workerFinishedPathList) {
      String hostnamePartitionId = FilenameUtils.getName(finishedPath);
      JSONObject workerFinishedInfoObj = null;
      try {
        byte [] zkData =
            getZkExt().getData(finishedPath, false, null);
        workerFinishedInfoObj = new JSONObject(new String(zkData,
            Charset.defaultCharset()));
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
          globalStats.addOocLoadBytesCount(
              workerMetrics.getBytesLoadedFromDisk());
          globalStats.addOocStoreBytesCount(
              workerMetrics.getBytesStoredOnDisk());
          // Find the lowest percentage of graph in memory across all workers
          // for one superstep
          globalStats.setLowestGraphPercentageInMemory(
              Math.min(globalStats.getLowestGraphPercentageInMemory(),
                  (int) Math.round(
                      workerMetrics.getGraphPercentageInMemory())));
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

    allPartitionStatsList.clear();
    Iterable<PartitionStats> statsList = globalCommHandler.getAllPartitionStats(
        workerFinishedPathList.size(), getContext());
    for (PartitionStats partitionStats : statsList) {
      globalStats.addPartitionStats(partitionStats);
      allPartitionStatsList.add(partitionStats);
    }

    if (conf.metricsEnabled()) {
      if (GiraphConstants.METRICS_DIRECTORY.isDefaultValue(conf)) {
        aggregatedMetrics.print(superstep, System.err);
      } else {
        printAggregatedMetricsToHDFS(superstep, aggregatedMetrics);
      }
      for (MasterObserver observer : observers) {
        observer.superstepMetricsUpdate(
            superstep, aggregatedMetrics, allPartitionStatsList);
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
            CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX);
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
    finalizedOutputStream.writeUTF(getCheckpointBasePath(superstep));
    for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
      finalizedOutputStream.writeInt(getWorkerId(chosenWorkerInfo));
    }
    globalCommHandler.getAggregatorHandler().write(finalizedOutputStream);
    aggregatorTranslation.write(finalizedOutputStream);
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
   */
  private void assignPartitionOwners() {
    Collection<PartitionOwner> partitionOwners;
    if (getSuperstep() == INPUT_SUPERSTEP) {
      partitionOwners =
          masterGraphPartitioner.createInitialPartitionOwners(
              chosenWorkerInfoList, maxWorkers);
      if (partitionOwners.isEmpty()) {
        throw new IllegalStateException(
            "assignAndExchangePartitions: No partition owners set");
      }
    } else if (getRestartedSuperstep() == getSuperstep()) {
      // If restarted, prepare the checkpoint restart
      try {
        partitionOwners = prepareCheckpointRestart(getSuperstep());
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
      masterGraphPartitioner.setPartitionOwners(partitionOwners);
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

    AddressesAndPartitionsWritable addressesAndPartitions =
        new AddressesAndPartitionsWritable(masterInfo, chosenWorkerInfoList,
            partitionOwners);
    // Send assignments to every worker
    // TODO for very large number of partitions we might want to split this
    // across multiple requests
    for (WorkerInfo workerInfo : chosenWorkerInfoList) {
      masterClient.sendWritableRequest(workerInfo.getTaskId(),
          new AddressesAndPartitionsRequest(addressesAndPartitions));
    }
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
   * @return a list of dead workers. Empty list if all workers are alive.
   * @throws InterruptedException
   * @throws KeeperException
   */
  private Collection<WorkerInfo> superstepChosenWorkerAlive(
    String chosenWorkerInfoHealthPath,
    List<WorkerInfo> chosenWorkerInfoList)
    throws KeeperException, InterruptedException {
    List<WorkerInfo> chosenWorkerInfoHealthyList =
        getWorkerInfosFromPath(chosenWorkerInfoHealthPath, false);
    Set<WorkerInfo> chosenWorkerInfoHealthySet =
        new HashSet<WorkerInfo>(chosenWorkerInfoHealthyList);
    List<WorkerInfo> deadWorkers = new ArrayList<>();
    for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
      if (!chosenWorkerInfoHealthySet.contains(chosenWorkerInfo)) {
        deadWorkers.add(chosenWorkerInfo);
      }
    }
    return deadWorkers;
  }

  @Override
  public void restartFromCheckpoint(long checkpoint) {
    // Process:
    // 1. Increase the application attempt and set to the correct checkpoint
    // 2. Send command to all workers to restart their tasks
    setApplicationAttempt(getApplicationAttempt() + 1);
    setCachedSuperstep(checkpoint);
    setRestartedSuperstep(checkpoint);
    checkpointStatus = CheckpointStatus.NONE;
    setJobState(ApplicationState.START_SUPERSTEP,
        getApplicationAttempt(),
        checkpoint);
  }

  /**
   * Safely removes node from zookeeper.
   * Ignores if node is already removed. Can only throw runtime exception if
   * anything wrong.
   * @param path path to the node to be removed.
   */
  private void zkDeleteNode(String path) {
    try {
      getZkExt().deleteExt(path, -1, true);
    } catch (KeeperException.NoNodeException e) {
      LOG.info("zkDeleteNode: node has already been removed " + path);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "zkDeleteNode: InterruptedException", e);
    } catch (KeeperException e) {
      throw new RuntimeException(
          "zkDeleteNode: KeeperException", e);
    }
  }

  @Override
  public long getLastGoodCheckpoint() throws IOException {
    // Find the last good checkpoint if none have been written to the
    // knowledge of this master
    if (lastCheckpointedSuperstep == -1) {
      try {
        lastCheckpointedSuperstep = getLastCheckpointedSuperstep();
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
   * @param ignoreDeath In case if worker died after making it through
   *                    barrier, we will ignore death if set to true.
   * @return True if barrier was successful, false if there was a worker
   *         failure
   */
  private boolean barrierOnWorkerList(String finishedWorkerPath,
      List<WorkerInfo> workerInfoList,
      BspEvent event,
      boolean ignoreDeath) {
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
    List<String> finishedHostnameIdList = new ArrayList<>();
    List<String> tmpFinishedHostnameIdList;
    long nextInfoMillis = System.currentTimeMillis();
    final int defaultTaskTimeoutMsec = 10 * 60 * 1000;  // from TaskTracker
    final int waitBetweenLogInfoMsec = 30 * 1000;
    final int taskTimeoutMsec = getContext().getConfiguration().getInt(
        "mapred.task.timeout", defaultTaskTimeoutMsec) / 2;
    long lastRegularRunTimeMsec = 0;
    int eventLoopTimeout =  Math.min(taskTimeoutMsec, waitBetweenLogInfoMsec);
    boolean logInfoOnlyRun = false;
    List<WorkerInfo> deadWorkers = new ArrayList<>();
    while (true) {
      if (! logInfoOnlyRun) {
        try {
          tmpFinishedHostnameIdList =
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
          // Log the names of the new workers that have finished since last time
          Set<String> newFinishedHostnames = Sets.difference(
            Sets.newHashSet(tmpFinishedHostnameIdList),
            Sets.newHashSet(finishedHostnameIdList));
          LOG.debug("barrierOnWorkerList: Got new finished worker list = " +
                        newFinishedHostnames + ", size = " +
                        newFinishedHostnames.size() +
                        " from " + finishedWorkerPath);
        }
        finishedHostnameIdList = tmpFinishedHostnameIdList;
      }

      if (LOG.isInfoEnabled() &&
          (System.currentTimeMillis() > nextInfoMillis)) {
        nextInfoMillis = System.currentTimeMillis() + waitBetweenLogInfoMsec;
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

      if (! logInfoOnlyRun) {
        getContext().setStatus(getGraphTaskManager().getGraphFunctions() +
                                   " - " +
                                   finishedHostnameIdList.size() +
                                   " finished out of " +
                                   workerInfoList.size() +
                                   " on superstep " + getSuperstep());
        if (finishedHostnameIdList.containsAll(hostnameIdList)) {
          break;
        }

        for (WorkerInfo deadWorker : deadWorkers) {
          if (!finishedHostnameIdList.contains(deadWorker.getHostnameId())) {
            LOG.error("barrierOnWorkerList: no results arived from " +
                          "worker that was pronounced dead: " + deadWorker +
                          " on superstep " + getSuperstep());
            return false;
          }
        }

        // wall-clock time skew is ignored
        lastRegularRunTimeMsec = System.currentTimeMillis();
      }

      // Wait for a signal or timeout
      boolean eventTriggered = event.waitMsecs(eventLoopTimeout);

      // If the event was triggered, we reset it. In the next loop run, we will
      // read ZK to get the new hosts.
      if (eventTriggered) {
        event.reset();
      }

      long elapsedTimeSinceRegularRunMsec = System.currentTimeMillis() -
          lastRegularRunTimeMsec;
      getContext().progress();

      if (eventTriggered ||
          taskTimeoutMsec == eventLoopTimeout ||
          elapsedTimeSinceRegularRunMsec >= taskTimeoutMsec) {
        logInfoOnlyRun = false;
      } else {
        logInfoOnlyRun = true;
        continue;
      }

      // Did a worker die?
      try {
        deadWorkers.addAll(superstepChosenWorkerAlive(
                workerInfoHealthyPath,
                workerInfoList));
        if (!ignoreDeath && deadWorkers.size() > 0) {
          String errorMessage = "******* WORKERS " + deadWorkers +
              " FAILED *******";
          // If checkpointing is not used, we should fail the job
          if (!getConfiguration().useCheckpointing()) {
            setJobStateFailed(errorMessage);
          } else {
            LOG.error("barrierOnWorkerList: Missing chosen " +
                "workers " + deadWorkers +
                " on superstep " + getSuperstep());
            // Log worker failure to command line
            getGraphTaskManager().getJobProgressTracker().logInfo(errorMessage);
          }
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
   */
  private void coordinateInputSplits() {
    // Coordinate the workers finishing sending their vertices/edges to the
    // correct workers and signal when everything is done.
    if (!barrierOnWorkerList(inputSplitsWorkerDonePath,
        chosenWorkerInfoList,
        getInputSplitsWorkerDoneEvent(),
        false)) {
      throw new IllegalStateException("coordinateInputSplits: Worker failed " +
          "during input split (currently not supported)");
    }
    try {
      getZkExt().createExt(inputSplitsAllDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          false);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("coordinateInputSplits: Node " +
          inputSplitsAllDonePath + " already exists.");
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "coordinateInputSplits: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "coordinateInputSplits: IllegalStateException", e);
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
    globalCommHandler.getAggregatorHandler().prepareSuperstep();

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
    aggregatorTranslation.postMasterCompute();
    globalCommHandler.getAggregatorHandler().finishSuperstep();

    globalCommHandler.getAggregatorHandler().sendDataToOwners(masterClient);
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
        SuperstepClasses.createAndExtractTypes(getConfiguration());
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
      // Sort this list, so order stays the same over supersteps
      Collections.sort(chosenWorkerInfoList, new Comparator<WorkerInfo>() {
        @Override
        public int compare(WorkerInfo wi1, WorkerInfo wi2) {
          return Integer.compare(wi1.getTaskId(), wi2.getTaskId());
        }
      });
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

    // We need to finalize aggregators from previous superstep
    if (getSuperstep() >= 0) {
      aggregatorTranslation.postMasterCompute();
      globalCommHandler.getAggregatorHandler().finishSuperstep();
    }

    masterClient.openConnections();

    GiraphStats.getInstance().
        getCurrentWorkers().setValue(chosenWorkerInfoList.size());
    assignPartitionOwners();

    // Finalize the valid checkpoint file prefixes and possibly
    // the aggregators.
    if (checkpointStatus != CheckpointStatus.NONE) {
      String workerWroteCheckpointPath =
          getWorkerWroteCheckpointPath(getApplicationAttempt(),
              getSuperstep());
      // first wait for all the workers to write their checkpoint data
      if (!barrierOnWorkerList(workerWroteCheckpointPath,
          chosenWorkerInfoList,
          getWorkerWroteCheckpointEvent(),
          checkpointStatus == CheckpointStatus.CHECKPOINT_AND_HALT)) {
        return SuperstepState.WORKER_FAILURE;
      }
      try {
        finalizeCheckpoint(getSuperstep(), chosenWorkerInfoList);
      } catch (IOException e) {
        throw new IllegalStateException(
            "coordinateSuperstep: IOException on finalizing checkpoint",
            e);
      }
      if (checkpointStatus == CheckpointStatus.CHECKPOINT_AND_HALT) {
        return SuperstepState.CHECKPOINT_AND_HALT;
      }
    }

    // We need to send aggregators to worker owners after new worker assignments
    if (getSuperstep() >= 0) {
      globalCommHandler.getAggregatorHandler().sendDataToOwners(masterClient);
    }

    if (getSuperstep() == INPUT_SUPERSTEP) {
      // Initialize aggregators before coordinating
      initializeAggregatorInputSuperstep();
      coordinateInputSplits();
    }

    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep());
    if (!barrierOnWorkerList(finishedWorkerPath,
        chosenWorkerInfoList,
        getSuperstepStateChangedEvent(),
        false)) {
      return SuperstepState.WORKER_FAILURE;
    }

    // Collect aggregator values, then run the master.compute() and
    // finally save the aggregator values
    globalCommHandler.getAggregatorHandler().prepareSuperstep();
    aggregatorTranslation.prepareSuperstep();

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
      superstepClasses.verifyTypesMatch(getSuperstep() > 0);
    }
    getConfiguration().updateSuperstepClasses(superstepClasses);

    //Signal workers that we want to checkpoint
    checkpointStatus = getCheckpointStatus(getSuperstep() + 1);
    globalStats.setCheckpointStatus(checkpointStatus);
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
    globalCommHandler.getAggregatorHandler().writeAggregators(
        getSuperstep(), superstepState);

    return superstepState;
  }

  /**
   * Should checkpoint on this superstep?  If checkpointing, always
   * checkpoint the first user superstep.  If restarting, the first
   * checkpoint is after the frequency has been met.
   *
   * @param superstep Decide if checkpointing no this superstep
   * @return True if this superstep should be checkpointed, false otherwise
   */
  private CheckpointStatus getCheckpointStatus(long superstep) {
    try {
      if (getZkExt().
          exists(basePath + FORCE_CHECKPOINT_USER_FLAG, false) != null) {
        if (isCheckpointingSupported(getConfiguration(), masterCompute)) {
          return CheckpointStatus.CHECKPOINT_AND_HALT;
        } else {
          LOG.warn("Attempted to manually checkpoint the job that " +
              "does not support checkpoints. Ignoring");
        }
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "cleanupZooKeeper: Got KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "cleanupZooKeeper: Got IllegalStateException", e);
    }
    if (checkpointFrequency == 0) {
      return CheckpointStatus.NONE;
    }
    long firstCheckpoint = INPUT_SUPERSTEP + 1;
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      firstCheckpoint = getRestartedSuperstep() + checkpointFrequency;
    }
    if (superstep < firstCheckpoint) {
      return CheckpointStatus.NONE;
    }
    if (((superstep - firstCheckpoint) % checkpointFrequency) == 0) {
      if (isCheckpointingSupported(getConfiguration(), masterCompute)) {
        return CheckpointStatus.CHECKPOINT;
      }
    }
    return CheckpointStatus.NONE;
  }

  /**
   * Returns false if job doesn't support checkpoints.
   * Job may not support checkpointing if it does output during
   * computation, uses static variables to keep data between supersteps,
   * starts new threads etc.
   * @param conf Immutable configuration of the job
   * @param masterCompute instance of master compute
   * @return true if it is safe to checkpoint the job
   */
  private boolean isCheckpointingSupported(
      GiraphConfiguration conf, MasterCompute masterCompute) {
    return checkpointSupportedChecker.isCheckpointSupported(
        conf, masterCompute);
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

      getCleanedUpChildrenChangedEvent().waitForTimeoutOrFail(
          GiraphConstants.WAIT_FOR_OTHER_WORKERS_TIMEOUT_MSEC.get(
              getConfiguration()));
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
  public void cleanup(SuperstepState superstepState) throws IOException {
    ImmutableClassesGiraphConfiguration conf = getConfiguration();

    // All master processes should denote they are done by adding special
    // znode.  Once the number of znodes equals the number of partitions
    // for workers and masters, the master will clean up the ZooKeeper
    // znodes associated with this job.
    String masterCleanedUpPath = cleanedUpPath  + "/" +
        getTaskId() + MASTER_SUFFIX;
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
      if (superstepState == SuperstepState.ALL_SUPERSTEPS_DONE &&
          GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS.get(conf)) {
        boolean success =
            getFs().delete(new Path(checkpointBasePath), true);
        if (LOG.isInfoEnabled()) {
          LOG.info("cleanup: Removed HDFS checkpoint directory (" +
              checkpointBasePath + ") with return = " +
              success + " since the job " + getContext().getJobName() +
              " succeeded ");
        }
      }
      if (superstepState == SuperstepState.CHECKPOINT_AND_HALT) {
        getFs().create(CheckpointingUtils.getCheckpointMarkPath(conf,
            getJobId()), true);
        failJob(new Exception("Checkpoint and halt requested. " +
            "Killing this job."));
      }
      globalCommHandler.getAggregatorHandler().close();
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
    gs.getAggregateSentMessages().increment(globalStats.getMessageCount());
    gs.getAggregateSentMessageBytes()
      .increment(globalStats.getMessageBytesCount());
    gs.getAggregateOOCBytesLoaded()
      .increment(globalStats.getOocLoadBytesCount());
    gs.getAggregateOOCBytesStored()
      .increment(globalStats.getOocStoreBytesCount());
    // Updating the lowest percentage of graph in memory throughout the
    // execution across all the supersteps
    int percentage = (int) gs.getLowestGraphPercentageInMemory().getValue();
    gs.getLowestGraphPercentageInMemory().setValue(
        Math.min(percentage, globalStats.getLowestGraphPercentageInMemory()));
  }
}
