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

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.comm.MasterClientServer;
import org.apache.giraph.comm.netty.NettyMasterClientServer;
import org.apache.giraph.graph.GraphMapper.MapFunctions;
import org.apache.giraph.graph.partition.MasterGraphPartitioner;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.PartitionUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.WritableUtils;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
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

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceMaster}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class BspServiceMaster<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BspService<I, V, E, M>
    implements CentralizedServiceMaster<I, V, E, M> {
  /** Counter group name for the Giraph statistics */
  public static final String GIRAPH_STATS_COUNTER_GROUP_NAME = "Giraph Stats";
  /** Print worker names only if there are 10 workers left */
  public static final int MAX_PRINTABLE_REMAINING_WORKERS = 10;
  /** How many threads to use when writing input splits to zookeeper*/
  public static final String INPUT_SPLIT_THREAD_COUNT =
      "giraph.inputSplitThreadCount";
  /** Default number of threads to use when writing input splits to zookeeper */
  public static final int DEFAULT_INPUT_SPLIT_THREAD_COUNT = 1;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
  /** Superstep counter */
  private Counter superstepCounter = null;
  /** Vertex counter */
  private Counter vertexCounter = null;
  /** Finished vertex counter */
  private Counter finishedVertexCounter = null;
  /** Edge counter */
  private Counter edgeCounter = null;
  /** Sent messages counter */
  private Counter sentMessagesCounter = null;
  /** Workers on this superstep */
  private Counter currentWorkersCounter = null;
  /** Current master task partition */
  private Counter currentMasterTaskPartitionCounter = null;
  /** Last checkpointed superstep */
  private Counter lastCheckpointedSuperstepCounter = null;
  /** Am I the master? */
  private boolean isMaster = false;
  /** Max number of workers */
  private final int maxWorkers;
  /** Min number of workers */
  private final int minWorkers;
  /** Min % responded workers */
  private final float minPercentResponded;
  /** Poll period in msecs */
  private final int msecsPollPeriod;
  /** Max number of poll attempts */
  private final int maxPollAttempts;
  /** Min number of long tails before printing */
  private final int partitionLongTailMinPrint;
  /** Last finalized checkpoint */
  private long lastCheckpointedSuperstep = -1;
  /** Worker wrote checkpoint */
  private final BspEvent workerWroteCheckpoint;
  /** State of the superstep changed */
  private final BspEvent superstepStateChanged;
  /** Master graph partitioner */
  private final MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner;
  /** All the partition stats from the last superstep */
  private final List<PartitionStats> allPartitionStatsList =
      new ArrayList<PartitionStats>();
  /** Handler for aggregators */
  private MasterAggregatorHandler aggregatorHandler;
  /** Master class */
  private MasterCompute masterCompute;
  /** Communication service */
  private MasterClientServer commService;
  /** Master info */
  private WorkerInfo masterInfo;
  /** List of workers in current superstep */
  private List<WorkerInfo> chosenWorkerInfoList = Lists.newArrayList();
  /** Limit locality information added to each InputSplit znode */
  private final int localityLimit = 5;

  /**
   * Constructor for setting up the master.
   *
   * @param serverPortList ZooKeeper server port list
   * @param sessionMsecTimeout Msecs to timeout connecting to ZooKeeper
   * @param context Mapper context
   * @param graphMapper Graph mapper
   */
  public BspServiceMaster(
      String serverPortList,
      int sessionMsecTimeout,
      Mapper<?, ?, ?, ?>.Context context,
      GraphMapper<I, V, E, M> graphMapper) {
    super(serverPortList, sessionMsecTimeout, context, graphMapper);
    workerWroteCheckpoint = new PredicateLock(context);
    registerBspEvent(workerWroteCheckpoint);
    superstepStateChanged = new PredicateLock(context);
    registerBspEvent(superstepStateChanged);

    maxWorkers =
        getConfiguration().getInt(GiraphConfiguration.MAX_WORKERS, -1);
    minWorkers =
        getConfiguration().getInt(GiraphConfiguration.MIN_WORKERS, -1);
    minPercentResponded =
        getConfiguration().getFloat(GiraphConfiguration.MIN_PERCENT_RESPONDED,
            100.0f);
    msecsPollPeriod =
        getConfiguration().getInt(GiraphConfiguration.POLL_MSECS,
            GiraphConfiguration.POLL_MSECS_DEFAULT);
    maxPollAttempts =
        getConfiguration().getInt(GiraphConfiguration.POLL_ATTEMPTS,
            GiraphConfiguration.POLL_ATTEMPTS_DEFAULT);
    partitionLongTailMinPrint = getConfiguration().getInt(
        GiraphConfiguration.PARTITION_LONG_TAIL_MIN_PRINT,
        GiraphConfiguration.PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT);
    masterGraphPartitioner =
        getGraphPartitionerFactory().createMasterGraphPartitioner();
  }

  @Override
  public void setJobState(ApplicationState state,
      long applicationAttempt,
      long desiredSuperstep) {
    JSONObject jobState = new JSONObject();
    try {
      jobState.put(JSONOBJ_STATE_KEY, state.toString());
      jobState.put(JSONOBJ_APPLICATION_ATTEMPT_KEY, applicationAttempt);
      jobState.put(JSONOBJ_SUPERSTEP_KEY, desiredSuperstep);
    } catch (JSONException e) {
      throw new RuntimeException("setJobState: Coudn't put " +
          state.toString());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("setJobState: " + jobState.toString() + " on superstep " +
          getSuperstep());
    }
    try {
      getZkExt().createExt(masterJobStatePath + "/jobState",
          jobState.toString().getBytes(),
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

    if (state == ApplicationState.FAILED) {
      failJob();
    }
  }

  /**
   * Master uses this to calculate the {@link VertexInputFormat}
   * input splits and write it to ZooKeeper.
   *
   * @param numWorkers Number of available workers
   * @return List of input splits
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private List<InputSplit> generateInputSplits(int numWorkers) {
    VertexInputFormat<I, V, E, M> vertexInputFormat =
        getConfiguration().createVertexInputFormat();
    List<InputSplit> splits;
    try {
      splits = vertexInputFormat.getSplits(getContext(), numWorkers);
      float samplePercent =
          getConfiguration().getFloat(
              GiraphConfiguration.INPUT_SPLIT_SAMPLE_PERCENT,
              GiraphConfiguration.INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT);
      if (samplePercent !=
          GiraphConfiguration.INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT) {
        int lastIndex = (int) (samplePercent * splits.size() / 100f);
        List<InputSplit> sampleSplits = splits.subList(0, lastIndex);
        LOG.warn("generateInputSplits: Using sampling - Processing " +
            "only " + sampleSplits.size() + " instead of " +
            splits.size() + " expected splits.");
        return sampleSplits;
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("generateInputSplits: Got " + splits.size() +
              " input splits for " + numWorkers + " workers");
        }
        return splits;
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "generateInputSplits: Got IOException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "generateInputSplits: Got InterruptedException", e);
    }
  }

  /**
   * When there is no salvaging this job, fail it.
   *
   * @throws IOException
   */
  private void failJob() {
    LOG.fatal("failJob: Killing job " + getJobId());
    try {
      @SuppressWarnings("deprecation")
      org.apache.hadoop.mapred.JobClient jobClient =
          new org.apache.hadoop.mapred.JobClient(
              (org.apache.hadoop.mapred.JobConf)
              getContext().getConfiguration());
      @SuppressWarnings("deprecation")
      org.apache.hadoop.mapred.JobID jobId =
          org.apache.hadoop.mapred.JobID.forName(getJobId());
      RunningJob job = jobClient.getJob(jobId);
      job.killJob();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
      throw new IllegalStateException("getWorkers: IllegalStateException", e);
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
      throw new IllegalStateException("getWorkers: IllegalStateException", e);
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
    int pollAttempt = 0;
    List<WorkerInfo> healthyWorkerInfoList = new ArrayList<WorkerInfo>();
    List<WorkerInfo> unhealthyWorkerInfoList = new ArrayList<WorkerInfo>();
    int totalResponses = -1;
    while (pollAttempt < maxPollAttempts) {
      getAllWorkerInfos(
          getSuperstep(), healthyWorkerInfoList, unhealthyWorkerInfoList);
      totalResponses = healthyWorkerInfoList.size() +
          unhealthyWorkerInfoList.size();
      if ((totalResponses * 100.0f / maxWorkers) >=
          minPercentResponded) {
        failJob = false;
        break;
      }
      getContext().setStatus(getGraphMapper().getMapFunctions() + " " +
          "checkWorkers: Only found " +
          totalResponses +
          " responses of " + maxWorkers +
          " needed to start superstep " +
          getSuperstep());
      if (getWorkerHealthRegistrationChangedEvent().waitMsecs(
          msecsPollPeriod)) {
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
            getSuperstep() + ".  Sleeping for " +
            msecsPollPeriod + " msecs and used " + pollAttempt +
            " of " + maxPollAttempts + " attempts.");
        // Find the missing workers if there are only a few
        if ((maxWorkers - totalResponses) <=
            partitionLongTailMinPrint) {
          logMissingWorkersOnSuperstep(healthyWorkerInfoList,
              unhealthyWorkerInfoList);
        }
      }
      ++pollAttempt;
    }
    if (failJob) {
      LOG.error("checkWorkers: Did not receive enough processes in " +
          "time (only " + totalResponses + " of " +
          minWorkers + " required).  This occurs if you do not " +
          "have enough map tasks available simultaneously on " +
          "your Hadoop instance to fulfill the number of " +
          "requested workers.");
      return null;
    }

    if (healthyWorkerInfoList.size() < minWorkers) {
      LOG.error("checkWorkers: Only " + healthyWorkerInfoList.size() +
          " available when " + minWorkers + " are required.");
      logMissingWorkersOnSuperstep(healthyWorkerInfoList,
          unhealthyWorkerInfoList);
      return null;
    }

    getContext().setStatus(getGraphMapper().getMapFunctions() + " " +
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

  @Override
  public int createInputSplits() {
    // Only the 'master' should be doing this.  Wait until the number of
    // processes that have reported health exceeds the minimum percentage.
    // If the minimum percentage is not met, fail the job.  Otherwise
    // generate the input splits
    try {
      if (getZkExt().exists(inputSplitsPath, false) != null) {
        LOG.info(inputSplitsPath +
            " already exists, no need to create");
        return Integer.parseInt(
            new String(
                getZkExt().getData(inputSplitsPath, false, null)));
      }
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("createInputSplits: Need to create the " +
            "input splits at " + inputSplitsPath);
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "createInputSplits: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "createInputSplits: InterrtupedException", e);
    }

    // When creating znodes, in case the master has already run, resume
    // where it left off.
    List<WorkerInfo> healthyWorkerInfoList = checkWorkers();
    if (healthyWorkerInfoList == null) {
      setJobState(ApplicationState.FAILED, -1, -1);
      return -1;
    }

    // Note that the input splits may only be a sample if
    // INPUT_SPLIT_SAMPLE_PERCENT is set to something other than 100
    List<InputSplit> splitList =
        generateInputSplits(healthyWorkerInfoList.size());
    if (splitList.isEmpty()) {
      LOG.fatal("createInputSplits: Failing job due to 0 input splits, " +
          "check input of " +
          getConfiguration().getVertexInputFormatClass().getName() + "!");
      getContext().setStatus("Failing job due to 0 input splits, " +
          "check input of " +
          getConfiguration().getVertexInputFormatClass().getName() + "!");
      failJob();
    }
    if (healthyWorkerInfoList.size() > splitList.size()) {
      LOG.warn("createInputSplits: Number of inputSplits=" +
          splitList.size() + " < " +
          healthyWorkerInfoList.size() +
          "=number of healthy processes, " +
          "some workers will be not used");
    }

    // Write input splits to zookeeper in parallel
    int inputSplitThreadCount = getConfiguration().getInt(
        INPUT_SPLIT_THREAD_COUNT,
        DEFAULT_INPUT_SPLIT_THREAD_COUNT);
    if (LOG.isInfoEnabled()) {
      LOG.info("createInputSplits: Starting to write input split data to " +
          "zookeeper with " + inputSplitThreadCount + " threads");
    }
    ExecutorService taskExecutor =
        Executors.newFixedThreadPool(inputSplitThreadCount);
    for (int i = 0; i < splitList.size(); ++i) {
      InputSplit inputSplit = splitList.get(i);
      taskExecutor.submit(new WriteInputSplit(inputSplit, i));
    }
    taskExecutor.shutdown();
    ProgressableUtils.awaitExecutorTermination(taskExecutor, getContext());
    if (LOG.isInfoEnabled()) {
      LOG.info("createInputSplits: Done writing input split data to zookeeper");
    }

    // Let workers know they can start trying to load the input splits
    try {
      getZkExt().create(inputSplitsAllReadyPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("createInputSplits: Node " +
          inputSplitsAllReadyPath + " already exists.");
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "createInputSplits: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "createInputSplits: IllegalStateException", e);
    }

    return splitList.size();
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return chosenWorkerInfoList;
  }

  @Override
  public MasterAggregatorUsage getAggregatorUsage() {
    return aggregatorHandler;
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
    int prefixFileCount = finalizedStream.readInt();
    for (int i = 0; i < prefixFileCount; ++i) {
      String metadataFilePath =
          finalizedStream.readUTF() + CHECKPOINT_METADATA_POSTFIX;
      validMetadataPathList.add(new Path(metadataFilePath));
    }

    aggregatorHandler.readFields(finalizedStream);
    aggregatorHandler.finishSuperstep(superstep - 1, this);
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
    superstepCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Superstep");
    vertexCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate vertices");
    finishedVertexCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate finished vertices");
    edgeCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate edges");
    sentMessagesCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Sent messages");
    currentWorkersCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Current workers");
    currentMasterTaskPartitionCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Current master task partition");
    lastCheckpointedSuperstepCounter = getContext().getCounter(
        GIRAPH_STATS_COUNTER_GROUP_NAME, "Last checkpointed superstep");
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      superstepCounter.increment(getRestartedSuperstep());
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
          currentMasterTaskPartitionCounter.increment(
              getTaskPartition() -
              currentMasterTaskPartitionCounter.getValue());
          masterCompute = getConfiguration().createMasterCompute();
          aggregatorHandler = new MasterAggregatorHandler(getConfiguration());
          aggregatorHandler.initialize(this);

          commService = new NettyMasterClientServer(
              getContext(), getConfiguration(), this);
          masterInfo = new WorkerInfo(getHostname(), getTaskPartition(),
              commService.getMyAddress().getPort());

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

  /**
   * Collect and aggregate the worker statistics for a particular superstep.
   *
   * @param superstep Superstep to aggregate on
   * @return Global statistics aggregated on all worker statistics
   */
  private GlobalStats aggregateWorkerStats(long superstep) {
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

    allPartitionStatsList.clear();
    for (String finishedPath : workerFinishedPathList) {
      JSONObject workerFinishedInfoObj = null;
      try {
        byte [] zkData =
            getZkExt().getData(finishedPath, false, null);
        workerFinishedInfoObj = new JSONObject(new String(zkData));
        List<PartitionStats> statsList =
            WritableUtils.readListFieldsFromByteArray(
                Base64.decode(workerFinishedInfoObj.getString(
                    JSONOBJ_PARTITION_STATS_KEY)),
                    partitionStatsClass,
                    getConfiguration());
        for (PartitionStats partitionStats : statsList) {
          globalStats.addPartitionStats(partitionStats);
          allPartitionStatsList.add(partitionStats);
        }
        globalStats.addMessageCount(
            workerFinishedInfoObj.getLong(
                JSONOBJ_NUM_MESSAGES_KEY));
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

    if (LOG.isInfoEnabled()) {
      LOG.info("aggregateWorkerStats: Aggregation found " + globalStats +
          " on superstep = " + getSuperstep());
    }
    return globalStats;
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
    lastCheckpointedSuperstepCounter.increment(superstep -
        lastCheckpointedSuperstepCounter.getValue());
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
      MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner) {
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
      getZkExt().deleteExt(inputSplitsPath, -1, true);
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
        failJob();
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
      getContext().setStatus(getGraphMapper().getMapFunctions() + " - " +
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
    if (!(getConfiguration().getBoolean(
        GiraphConfiguration.KEEP_ZOOKEEPER_DATA,
        GiraphConfiguration.KEEP_ZOOKEEPER_DATA_DEFAULT)) &&
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

    chosenWorkerInfoList = checkWorkers();
    if (chosenWorkerInfoList == null) {
      LOG.fatal("coordinateSuperstep: Not enough healthy workers for " +
          "superstep " + getSuperstep());
      setJobState(ApplicationState.FAILED, -1, -1);
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

    commService.openConnections();

    currentWorkersCounter.increment(chosenWorkerInfoList.size() -
        currentWorkersCounter.getValue());
    assignPartitionOwners(allPartitionStatsList,
        chosenWorkerInfoList,
        masterGraphPartitioner);

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
      // Coordinate the workers finishing sending their vertices to the
      // correct workers and signal when everything is done.
      if (!barrierOnWorkerList(inputSplitsDonePath,
          chosenWorkerInfoList,
          getInputSplitsDoneStateChangedEvent())) {
        throw new IllegalStateException(
            "coordinateSuperstep: Worker failed during input split " +
            "(currently not supported)");
      }
      try {
        getZkExt().create(inputSplitsAllDonePath,
            null,
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
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

    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep());
    if (!barrierOnWorkerList(finishedWorkerPath,
        chosenWorkerInfoList,
        getSuperstepStateChangedEvent())) {
      return SuperstepState.WORKER_FAILURE;
    }

    // Collect aggregator values, then run the master.compute() and
    // finally save the aggregator values
    aggregatorHandler.prepareSuperstep(getSuperstep(), this);
    runMasterCompute(getSuperstep());
    aggregatorHandler.finishSuperstep(getSuperstep(), this);

    // If the master is halted or all the vertices voted to halt and there
    // are no more messages in the system, stop the computation
    GlobalStats globalStats = aggregateWorkerStats(getSuperstep());
    if (masterCompute.isHalted() ||
        (globalStats.getFinishedVertexCount() ==
        globalStats.getVertexCount() &&
        globalStats.getMessageCount() == 0)) {
      globalStats.setHaltComputation(true);
    }

    // Let everyone know the aggregated application state through the
    // superstep finishing znode.
    String superstepFinishedNode =
        getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
    WritableUtils.writeToZnode(
        getZkExt(), superstepFinishedNode, -1, globalStats);
    updateCounters(globalStats);

    cleanUpOldSuperstep(getSuperstep() - 1);
    incrCachedSuperstep();
    // Counter starts at zero, so no need to increment
    if (getSuperstep() > 0) {
      superstepCounter.increment(1);
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
   * Run the master.compute() class
   *
   * @param superstep superstep for which to run the master.compute()
   */
  private void runMasterCompute(long superstep) {
    // The master.compute() should run logically before the workers, so
    // increase the superstep counter it uses by one
    GraphState<I, V, E, M> graphState =
        new GraphState<I, V, E, M>(superstep + 1, vertexCounter.getValue(),
            edgeCounter.getValue(), getContext(), getGraphMapper(), null);
    masterCompute.setGraphState(graphState);
    if (superstep == INPUT_SUPERSTEP) {
      try {
        masterCompute.initialize();
      } catch (InstantiationException e) {
        LOG.fatal("runMasterCompute: Failed in instantiation", e);
        throw new RuntimeException(
            "runMasterCompute: Failed in instantiation", e);
      } catch (IllegalAccessException e) {
        LOG.fatal("runMasterCompute: Failed in access", e);
        throw new RuntimeException(
            "runMasterCompute: Failed in access", e);
      }
    }
    masterCompute.compute();
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
    if ((getGraphMapper().getMapFunctions() == MapFunctions.ALL) ||
        (getGraphMapper().getMapFunctions() ==
        MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
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
      if (getConfiguration().getZookeeperList() != null &&
          !getConfiguration().getBoolean(
              GiraphConfiguration.KEEP_ZOOKEEPER_DATA,
              GiraphConfiguration.KEEP_ZOOKEEPER_DATA_DEFAULT)) {
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
  public void cleanup() throws IOException {
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
      cleanUpZooKeeper();
      // If desired, cleanup the checkpoint directory
      if (getConfiguration().getBoolean(
          GiraphConfiguration.CLEANUP_CHECKPOINTS_AFTER_SUCCESS,
          GiraphConfiguration.CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT)) {
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

      commService.closeConnections();
      commService.close();
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
    vertexCounter.increment(
        globalStats.getVertexCount() -
            vertexCounter.getValue());
    finishedVertexCounter.increment(
        globalStats.getFinishedVertexCount() -
            finishedVertexCounter.getValue());
    edgeCounter.increment(
        globalStats.getEdgeCount() -
            edgeCounter.getValue());
    sentMessagesCounter.increment(
        globalStats.getMessageCount() -
            sentMessagesCounter.getValue());
  }

  /**
   * Task that writes a given input split to zookeeper.
   * Upon failure call() throws an exception.
   */
  private class WriteInputSplit implements Callable<Void> {
    /** Input split which we are going to write */
    private final InputSplit inputSplit;
    /** Index of the input split */
    private final int index;

    /**
     * Constructor
     *
     * @param inputSplit Input split which we are going to write
     * @param index Index of the input split
     */
    public WriteInputSplit(InputSplit inputSplit, int index) {
      this.inputSplit = inputSplit;
      this.index = index;
    }

    @Override
    public Void call() {
      String inputSplitPath = null;
      try {
        ByteArrayOutputStream byteArrayOutputStream =
            new ByteArrayOutputStream();
        DataOutput outputStream =
            new DataOutputStream(byteArrayOutputStream);

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
        Text.writeString(outputStream,
            inputSplit.getClass().getName());
        ((Writable) inputSplit).write(outputStream);
        inputSplitPath = inputSplitsPath + "/" + index;
        getZkExt().createExt(inputSplitPath,
            byteArrayOutputStream.toByteArray(),
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT,
            true);

        if (LOG.isDebugEnabled()) {
          LOG.debug("call: Created input split " +
              "with index " + index + " serialized as " +
              byteArrayOutputStream.toString());
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
