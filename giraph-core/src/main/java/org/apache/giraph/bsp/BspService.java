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

package org.apache.giraph.bsp;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.job.JobProgressTracker;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.writable.kryo.GiraphClassResolver;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.giraph.conf.GiraphConstants.RESTART_JOB_ID;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public abstract class BspService<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements Watcher, CentralizedService<I, V, E> {
  /** Unset superstep */
  public static final long UNSET_SUPERSTEP = Long.MIN_VALUE;
  /** Input superstep (superstep when loading the vertices happens) */
  public static final long INPUT_SUPERSTEP = -1;
  /** Unset application attempt */
  public static final long UNSET_APPLICATION_ATTEMPT = Long.MIN_VALUE;
  /** Base ZooKeeper directory */
  public static final String BASE_DIR = "/_hadoopBsp";
  /** Master job state znode above base dir */
  public static final String MASTER_JOB_STATE_NODE = "/_masterJobState";

  /** Input splits worker done directory */
  public static final String INPUT_SPLITS_WORKER_DONE_DIR =
      "/_inputSplitsWorkerDoneDir";
  /** Input splits all done node*/
  public static final String INPUT_SPLITS_ALL_DONE_NODE =
      "/_inputSplitsAllDone";
  /** Directory to store kryo className-ID assignment */
  public static final String KRYO_REGISTERED_CLASS_DIR =
          "/_kryo";
  /** Directory of attempts of this application */
  public static final String APPLICATION_ATTEMPTS_DIR =
      "/_applicationAttemptsDir";
  /** Where the master election happens */
  public static final String MASTER_ELECTION_DIR = "/_masterElectionDir";
  /** Superstep scope */
  public static final String SUPERSTEP_DIR = "/_superstepDir";
  /** Healthy workers register here. */
  public static final String WORKER_HEALTHY_DIR = "/_workerHealthyDir";
  /** Unhealthy workers register here. */
  public static final String WORKER_UNHEALTHY_DIR = "/_workerUnhealthyDir";
  /** Workers which wrote checkpoint notify here */
  public static final String WORKER_WROTE_CHECKPOINT_DIR =
      "/_workerWroteCheckpointDir";
  /** Finished workers notify here */
  public static final String WORKER_FINISHED_DIR = "/_workerFinishedDir";
  /** Helps coordinate the partition exchnages */
  public static final String PARTITION_EXCHANGE_DIR =
      "/_partitionExchangeDir";
  /** Denotes that the superstep is done */
  public static final String SUPERSTEP_FINISHED_NODE = "/_superstepFinished";
  /** Denotes that computation should be halted */
  public static final String HALT_COMPUTATION_NODE = "/_haltComputation";
  /** Memory observer dir */
  public static final String MEMORY_OBSERVER_DIR = "/_memoryObserver";
  /** User sets this flag to checkpoint and stop the job */
  public static final String FORCE_CHECKPOINT_USER_FLAG = "/_checkpointAndStop";
  /** Denotes which workers have been cleaned up */
  public static final String CLEANED_UP_DIR = "/_cleanedUpDir";
  /** JSON message count key */
  public static final String JSONOBJ_NUM_MESSAGES_KEY = "_numMsgsKey";
  /** JSON message bytes count key */
  public static final String JSONOBJ_NUM_MESSAGE_BYTES_KEY = "_numMsgBytesKey";
  /** JSON metrics key */
  public static final String JSONOBJ_METRICS_KEY = "_metricsKey";

  /** JSON state key */
  public static final String JSONOBJ_STATE_KEY = "_stateKey";
  /** JSON application attempt key */
  public static final String JSONOBJ_APPLICATION_ATTEMPT_KEY =
      "_applicationAttemptKey";
  /** JSON superstep key */
  public static final String JSONOBJ_SUPERSTEP_KEY =
      "_superstepKey";
  /** Suffix denotes a worker */
  public static final String WORKER_SUFFIX = "_worker";
  /** Suffix denotes a master */
  public static final String MASTER_SUFFIX = "_master";

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspService.class);
  /** Path to the job's root */
  protected final String basePath;
  /** Path to the job state determined by the master (informative only) */
  protected final String masterJobStatePath;
  /** Input splits worker done directory */
  protected final String inputSplitsWorkerDonePath;
  /** Input splits all done node */
  protected final String inputSplitsAllDonePath;
  /** Path to the application attempts) */
  protected final String applicationAttemptsPath;
  /** Path to the cleaned up notifications */
  protected final String cleanedUpPath;
  /** Path to the checkpoint's root (including job id) */
  protected final String checkpointBasePath;
  /** Old checkpoint in case we want to restart some job */
  protected final String savedCheckpointBasePath;
  /** Path to the master election path */
  protected final String masterElectionPath;
  /** If this path exists computation will be halted */
  protected final String haltComputationPath;
  /** Path where memory observer stores data */
  protected final String memoryObserverPath;
  /** Kryo className-ID mapping directory */
  protected final String kryoRegisteredClassPath;
  /** Private ZooKeeper instance that implements the service */
  private final ZooKeeperExt zk;
  /** Has the Connection occurred? */
  private final BspEvent connectedEvent;
  /** Has worker registration changed (either healthy or unhealthy) */
  private final BspEvent workerHealthRegistrationChanged;
  /** Application attempt changed */
  private final BspEvent applicationAttemptChanged;
  /** Input splits worker done */
  private final BspEvent inputSplitsWorkerDoneEvent;
  /** Input splits all done */
  private final BspEvent inputSplitsAllDoneEvent;
  /** Superstep finished synchronization */
  private final BspEvent superstepFinished;
  /** Master election changed for any waited on attempt */
  private final BspEvent masterElectionChildrenChanged;
  /** Cleaned up directory children changed*/
  private final BspEvent cleanedUpChildrenChanged;
  /** Registered list of BspEvents */
  private final List<BspEvent> registeredBspEvents =
      new ArrayList<BspEvent>();
  /** Immutable configuration of the job*/
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Job context (mainly for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Cached superstep (from ZooKeeper) */
  private long cachedSuperstep = UNSET_SUPERSTEP;
  /** Restarted from a checkpoint (manual or automatic) */
  private long restartedSuperstep = UNSET_SUPERSTEP;
  /** Cached application attempt (from ZooKeeper) */
  private long cachedApplicationAttempt = UNSET_APPLICATION_ATTEMPT;
  /** Job id, to ensure uniqueness */
  private final String jobId;
  /** Task id, from partition and application attempt to ensure uniqueness */
  private final int taskId;
  /** My hostname */
  private final String hostname;
  /** Combination of hostname '_' task (unique id) */
  private final String hostnameTaskId;
  /** Graph partitioner */
  private final GraphPartitionerFactory<I, V, E> graphPartitionerFactory;
  /** Mapper that will do the graph computation */
  private final GraphTaskManager<I, V, E> graphTaskManager;
  /** File system */
  private final FileSystem fs;

  /**
   * Constructor.
   *
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   */
  public BspService(
      Mapper<?, ?, ?, ?>.Context context,
      GraphTaskManager<I, V, E> graphTaskManager) {
    this.connectedEvent = new PredicateLock(context);
    this.workerHealthRegistrationChanged = new PredicateLock(context);
    this.applicationAttemptChanged = new PredicateLock(context);
    this.inputSplitsWorkerDoneEvent = new PredicateLock(context);
    this.inputSplitsAllDoneEvent = new PredicateLock(context);
    this.superstepFinished = new PredicateLock(context);
    this.masterElectionChildrenChanged = new PredicateLock(context);
    this.cleanedUpChildrenChanged = new PredicateLock(context);

    registerBspEvent(connectedEvent);
    registerBspEvent(workerHealthRegistrationChanged);
    registerBspEvent(inputSplitsWorkerDoneEvent);
    registerBspEvent(inputSplitsAllDoneEvent);
    registerBspEvent(applicationAttemptChanged);
    registerBspEvent(superstepFinished);
    registerBspEvent(masterElectionChildrenChanged);
    registerBspEvent(cleanedUpChildrenChanged);

    this.context = context;
    this.graphTaskManager = graphTaskManager;
    this.conf = graphTaskManager.getConf();

    this.jobId = conf.getJobId();
    this.restartedSuperstep = conf.getLong(
        GiraphConstants.RESTART_SUPERSTEP, UNSET_SUPERSTEP);
    try {
      this.hostname = conf.getLocalHostname();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    this.graphPartitionerFactory = conf.createGraphPartitioner();

    basePath = ZooKeeperManager.getBasePath(conf) + BASE_DIR + "/" + jobId;
    getContext().getCounter(GiraphConstants.ZOOKEEPER_BASE_PATH_COUNTER_GROUP,
        basePath);
    masterJobStatePath = basePath + MASTER_JOB_STATE_NODE;
    inputSplitsWorkerDonePath = basePath + INPUT_SPLITS_WORKER_DONE_DIR;
    inputSplitsAllDonePath = basePath + INPUT_SPLITS_ALL_DONE_NODE;
    applicationAttemptsPath = basePath + APPLICATION_ATTEMPTS_DIR;
    cleanedUpPath = basePath + CLEANED_UP_DIR;
    kryoRegisteredClassPath = basePath + KRYO_REGISTERED_CLASS_DIR;


    String restartJobId = RESTART_JOB_ID.get(conf);

    savedCheckpointBasePath =
        CheckpointingUtils.getCheckpointBasePath(getConfiguration(),
            restartJobId == null ? getJobId() : restartJobId);

    checkpointBasePath = CheckpointingUtils.
        getCheckpointBasePath(getConfiguration(), getJobId());

    masterElectionPath = basePath + MASTER_ELECTION_DIR;
    String serverPortList = graphTaskManager.getZookeeperList();
    haltComputationPath = basePath + HALT_COMPUTATION_NODE;
    memoryObserverPath = basePath + MEMORY_OBSERVER_DIR;
    getContext().getCounter(GiraphConstants.ZOOKEEPER_HALT_NODE_COUNTER_GROUP,
        haltComputationPath);
    if (LOG.isInfoEnabled()) {
      LOG.info("BspService: Path to create to halt is " + haltComputationPath);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("BspService: Connecting to ZooKeeper with job " + jobId +
          ", partition " + conf.getTaskPartition() + " on " + serverPortList);
    }
    try {
      this.zk = new ZooKeeperExt(serverPortList,
                                 conf.getZooKeeperSessionTimeout(),
                                 conf.getZookeeperOpsMaxAttempts(),
                                 conf.getZookeeperOpsRetryWaitMsecs(),
                                 this,
                                 context);
      connectedEvent.waitForTimeoutOrFail(
          GiraphConstants.WAIT_ZOOKEEPER_TIMEOUT_MSEC.get(conf));
      this.fs = FileSystem.get(getConfiguration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    boolean disableGiraphResolver =
            GiraphConstants.DISABLE_GIRAPH_CLASS_RESOLVER.get(conf);
    if (!disableGiraphResolver) {
      GiraphClassResolver.setZookeeperInfo(zk, kryoRegisteredClassPath);
    }
    this.taskId = (int) getApplicationAttempt() * conf.getMaxWorkers() +
            conf.getTaskPartition();
    this.hostnameTaskId = hostname + "_" + getTaskId();

    //Trying to restart from the latest superstep
    if (restartJobId != null &&
        restartedSuperstep == UNSET_SUPERSTEP) {
      try {
        restartedSuperstep = getLastCheckpointedSuperstep();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    this.cachedSuperstep = restartedSuperstep;
    if ((restartedSuperstep != UNSET_SUPERSTEP) &&
        (restartedSuperstep < 0)) {
      throw new IllegalArgumentException(
          "BspService: Invalid superstep to restart - " +
              restartedSuperstep);
    }
  }

  /**
   * Get the superstep from a ZooKeeper path
   *
   * @param path Path to parse for the superstep
   * @return Superstep from the path.
   */
  public static long getSuperstepFromPath(String path) {
    int foundSuperstepStart = path.indexOf(SUPERSTEP_DIR);
    if (foundSuperstepStart == -1) {
      throw new IllegalArgumentException(
          "getSuperstepFromPath: Cannot find " + SUPERSTEP_DIR +
          "from " + path);
    }
    foundSuperstepStart += SUPERSTEP_DIR.length() + 1;
    int endIndex = foundSuperstepStart +
        path.substring(foundSuperstepStart).indexOf("/");
    if (endIndex == -1) {
      throw new IllegalArgumentException(
          "getSuperstepFromPath: Cannot find end of superstep from " +
              path);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("getSuperstepFromPath: Got path=" + path +
          ", start=" + foundSuperstepStart + ", end=" + endIndex);
    }
    return Long.parseLong(path.substring(foundSuperstepStart, endIndex));
  }

  /**
   * Get the hostname and id from a "healthy" worker path
   *
   * @param path Path to check
   * @return Hostname and id from path
   */
  public static String getHealthyHostnameIdFromPath(String path) {
    int foundWorkerHealthyStart = path.indexOf(WORKER_HEALTHY_DIR);
    if (foundWorkerHealthyStart == -1) {
      throw new IllegalArgumentException(
          "getHealthyHostnameidFromPath: Couldn't find " +
              WORKER_HEALTHY_DIR + " from " + path);
    }
    foundWorkerHealthyStart += WORKER_HEALTHY_DIR.length();
    return path.substring(foundWorkerHealthyStart);
  }

  /**
   * Generate the base superstep directory path for a given application
   * attempt
   *
   * @param attempt application attempt number
   * @return directory path based on the an attempt
   */
  public final String getSuperstepPath(long attempt) {
    return applicationAttemptsPath + "/" + attempt + SUPERSTEP_DIR;
  }

  /**
   * Generate the worker information "healthy" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerInfoHealthyPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_HEALTHY_DIR;
  }

  /**
   * Generate the worker information "unhealthy" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerInfoUnhealthyPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_UNHEALTHY_DIR;
  }

  /**
   * Generate the worker "wrote checkpoint" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerWroteCheckpointPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_WROTE_CHECKPOINT_DIR;
  }

  /**
   * Generate the worker "finished" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerFinishedPath(long attempt, long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_FINISHED_DIR;
  }

  /**
   * Generate the "partition exchange" directory path for a superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getPartitionExchangePath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + PARTITION_EXCHANGE_DIR;
  }

  /**
   * Based on the superstep, worker info, and attempt, get the appropriate
   * worker path for the exchange.
   *
   * @param attempt Application attempt
   * @param superstep Superstep
   * @param workerInfo Worker info of the exchange.
   * @return Path of the desired worker
   */
  public final String getPartitionExchangeWorkerPath(long attempt,
      long superstep,
      WorkerInfo workerInfo) {
    return getPartitionExchangePath(attempt, superstep) +
        "/" + workerInfo.getHostnameId();
  }

  /**
   * Generate the "superstep finished" directory path for a superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getSuperstepFinishedPath(long attempt, long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + SUPERSTEP_FINISHED_NODE;
  }

  /**
   * Generate the base superstep directory path for a given application
   * attempt
   *
   * @param superstep Superstep to use
   * @return Directory path based on the a superstep
   */
  public final String getCheckpointBasePath(long superstep) {
    return checkpointBasePath + "/" + superstep;
  }

  /**
   * In case when we restart another job this will give us a path
   * to saved checkpoint.
   * @param superstep superstep to use
   * @return Direcory path for restarted job based on the superstep
   */
  public final String getSavedCheckpointBasePath(long superstep) {
    return savedCheckpointBasePath + "/" + superstep;
  }


  /**
   * Get the ZooKeeperExt instance.
   *
   * @return ZooKeeperExt instance.
   */
  public final ZooKeeperExt getZkExt() {
    return zk;
  }

  @Override
  public final long getRestartedSuperstep() {
    return restartedSuperstep;
  }

  /**
   * Set the restarted superstep
   *
   * @param superstep Set the manually restarted superstep
   */
  public final void setRestartedSuperstep(long superstep) {
    if (superstep < INPUT_SUPERSTEP) {
      throw new IllegalArgumentException(
          "setRestartedSuperstep: Bad argument " + superstep);
    }
    restartedSuperstep = superstep;
  }

  /**
   * Get the file system
   *
   * @return file system
   */
  public final FileSystem getFs() {
    return fs;
  }

  public final ImmutableClassesGiraphConfiguration<I, V, E>
  getConfiguration() {
    return conf;
  }

  public final Mapper<?, ?, ?, ?>.Context getContext() {
    return context;
  }

  public final String getHostname() {
    return hostname;
  }

  public final String getHostnameTaskId() {
    return hostnameTaskId;
  }

  public final int getTaskId() {
    return taskId;
  }

  public final GraphTaskManager<I, V, E> getGraphTaskManager() {
    return graphTaskManager;
  }

  public final BspEvent getWorkerHealthRegistrationChangedEvent() {
    return workerHealthRegistrationChanged;
  }

  public final BspEvent getApplicationAttemptChangedEvent() {
    return applicationAttemptChanged;
  }

  public final BspEvent getInputSplitsWorkerDoneEvent() {
    return inputSplitsWorkerDoneEvent;
  }

  public final BspEvent getInputSplitsAllDoneEvent() {
    return inputSplitsAllDoneEvent;
  }

  public final BspEvent getSuperstepFinishedEvent() {
    return superstepFinished;
  }


  public final BspEvent getMasterElectionChildrenChangedEvent() {
    return masterElectionChildrenChanged;
  }

  public final BspEvent getCleanedUpChildrenChangedEvent() {
    return cleanedUpChildrenChanged;
  }

  /**
   * Get the master commanded job state as a JSONObject.  Also sets the
   * watches to see if the master commanded job state changes.
   *
   * @return Last job state or null if none
   */
  public final JSONObject getJobState() {
    try {
      getZkExt().createExt(masterJobStatePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("getJobState: Job state already exists (" +
          masterJobStatePath + ")");
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to create job state path " +
          "due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Failed to create job state path " +
          "due to InterruptedException", e);
    }
    String jobState = null;
    try {
      List<String> childList =
          getZkExt().getChildrenExt(
              masterJobStatePath, true, true, true);
      if (childList.isEmpty()) {
        return null;
      }
      jobState =
          new String(getZkExt().getData(childList.get(childList.size() - 1),
              true, null), Charset.defaultCharset());
    } catch (KeeperException.NoNodeException e) {
      LOG.info("getJobState: Job state path is empty! - " +
          masterJobStatePath);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to get job state path " +
          "children due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Failed to get job state path " +
          "children due to InterruptedException", e);
    }
    try {
      return new JSONObject(jobState);
    } catch (JSONException e) {
      throw new RuntimeException(
          "getJobState: Failed to parse job state " + jobState);
    }
  }

  /**
   * Get the job id
   *
   * @return job id
   */
  public final String getJobId() {
    return jobId;
  }

  /**
   * Get the latest application attempt and cache it.
   *
   * @return the latest application attempt
   */
  public final long getApplicationAttempt() {
    if (cachedApplicationAttempt != UNSET_APPLICATION_ATTEMPT) {
      return cachedApplicationAttempt;
    }
    try {
      getZkExt().createExt(applicationAttemptsPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("getApplicationAttempt: Node " +
          applicationAttemptsPath + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Couldn't create application " +
          "attempts path due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Couldn't create application " +
          "attempts path due to InterruptedException", e);
    }
    try {
      List<String> attemptList =
          getZkExt().getChildrenExt(
              applicationAttemptsPath, true, false, false);
      if (attemptList.isEmpty()) {
        cachedApplicationAttempt = 0;
      } else {
        cachedApplicationAttempt =
            Long.parseLong(Collections.max(attemptList));
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("Couldn't get application " +
          "attempts to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Couldn't get application " +
          "attempts to InterruptedException", e);
    }

    return cachedApplicationAttempt;
  }

  /**
   * Get the latest superstep and cache it.
   *
   * @return the latest superstep
   */
  public final long getSuperstep() {
    if (cachedSuperstep != UNSET_SUPERSTEP) {
      return cachedSuperstep;
    }
    String superstepPath = getSuperstepPath(getApplicationAttempt());
    try {
      getZkExt().createExt(superstepPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("getApplicationAttempt: Node " +
            applicationAttemptsPath + " already exists!");
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getSuperstep: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getSuperstep: InterruptedException", e);
    }

    List<String> superstepList;
    try {
      superstepList =
          getZkExt().getChildrenExt(superstepPath, true, false, false);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getSuperstep: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getSuperstep: InterruptedException", e);
    }
    if (superstepList.isEmpty()) {
      cachedSuperstep = INPUT_SUPERSTEP;
    } else {
      cachedSuperstep =
          Long.parseLong(Collections.max(superstepList));
    }

    return cachedSuperstep;
  }

  /**
   * Increment the cached superstep.  Shouldn't be the initial value anymore.
   */
  public final void incrCachedSuperstep() {
    if (cachedSuperstep == UNSET_SUPERSTEP) {
      throw new IllegalStateException(
          "incrSuperstep: Invalid unset cached superstep " +
              UNSET_SUPERSTEP);
    }
    ++cachedSuperstep;
  }

  /**
   * Set the cached superstep (should only be used for loading checkpoints
   * or recovering from failure).
   *
   * @param superstep will be used as the next superstep iteration
   */
  public final void setCachedSuperstep(long superstep) {
    cachedSuperstep = superstep;
  }

  /**
   * Set the cached application attempt (should only be used for restart from
   * failure by the master)
   *
   * @param applicationAttempt Will denote the new application attempt
   */
  public final void setApplicationAttempt(long applicationAttempt) {
    cachedApplicationAttempt = applicationAttempt;
    String superstepPath = getSuperstepPath(cachedApplicationAttempt);
    try {
      getZkExt().createExt(superstepPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      throw new IllegalArgumentException(
          "setApplicationAttempt: Attempt already exists! - " +
              superstepPath, e);
    } catch (KeeperException e) {
      throw new RuntimeException(
          "setApplicationAttempt: KeeperException - " +
              superstepPath, e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "setApplicationAttempt: InterruptedException - " +
              superstepPath, e);
    }
  }

  /**
   * Register a BspEvent.  Ensure that it will be signaled
   * by catastrophic failure so that threads waiting on an event signal
   * will be unblocked.
   *
   * @param event Event to be registered.
   */
  public void registerBspEvent(BspEvent event) {
    registeredBspEvents.add(event);
  }

  /**
   * Subclasses can use this to instantiate their respective partitioners
   *
   * @return Instantiated graph partitioner factory
   */
  protected GraphPartitionerFactory<I, V, E> getGraphPartitionerFactory() {
    return graphPartitionerFactory;
  }

  /**
   * Derived classes that want additional ZooKeeper events to take action
   * should override this.
   *
   * @param event Event that occurred
   * @return true if the event was processed here, false otherwise
   */
  protected boolean processEvent(WatchedEvent event) {
    return false;
  }

  @Override
  public final void process(WatchedEvent event) {
    // 1. Process all shared events
    // 2. Process specific derived class events
    if (LOG.isDebugEnabled()) {
      LOG.debug("process: Got a new event, path = " + event.getPath() +
          ", type = " + event.getType() + ", state = " +
          event.getState());
    }

    if ((event.getPath() == null) && (event.getType() == EventType.None)) {
      if (event.getState() == KeeperState.Disconnected) {
        // Watches may not be triggered for some time, so signal all BspEvents
        for (BspEvent bspEvent : registeredBspEvents) {
          bspEvent.signal();
        }
        LOG.warn("process: Disconnected from ZooKeeper (will automatically " +
            "try to recover) " + event);
      } else if (event.getState() == KeeperState.SyncConnected) {
        if (LOG.isInfoEnabled()) {
          LOG.info("process: Asynchronous connection complete.");
        }
        connectedEvent.signal();
      } else {
        LOG.warn("process: Got unknown null path event " + event);
      }
      return;
    }

    boolean eventProcessed = false;
    if (event.getPath().startsWith(masterJobStatePath)) {
      // This will cause all becomeMaster() MasterThreads to notice the
      // change in job state and quit trying to become the master.
      masterElectionChildrenChanged.signal();
      eventProcessed = true;
    } else if ((event.getPath().contains(WORKER_HEALTHY_DIR) ||
        event.getPath().contains(WORKER_UNHEALTHY_DIR)) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: workerHealthRegistrationChanged " +
            "(worker health reported - healthy/unhealthy )");
      }
      workerHealthRegistrationChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(INPUT_SPLITS_ALL_DONE_NODE) &&
        event.getType() == EventType.NodeCreated) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: all input splits done");
      }
      inputSplitsAllDoneEvent.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(INPUT_SPLITS_WORKER_DONE_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: worker done reading input splits");
      }
      inputSplitsWorkerDoneEvent.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(SUPERSTEP_FINISHED_NODE) &&
        event.getType() == EventType.NodeCreated) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: superstepFinished signaled");
      }
      superstepFinished.signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(applicationAttemptsPath) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: applicationAttemptChanged signaled");
      }
      applicationAttemptChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(MASTER_ELECTION_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: masterElectionChildrenChanged signaled");
      }
      masterElectionChildrenChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().equals(cleanedUpPath) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: cleanedUpChildrenChanged signaled");
      }
      cleanedUpChildrenChanged.signal();
      eventProcessed = true;
    }

    if (!(processEvent(event)) && (!eventProcessed)) {
      LOG.warn("process: Unknown and unprocessed event (path=" +
          event.getPath() + ", type=" + event.getType() +
          ", state=" + event.getState() + ")");
    }
  }

  /**
   * Get the last saved superstep.
   *
   * @return Last good superstep number
   * @throws IOException
   */
  protected long getLastCheckpointedSuperstep() throws IOException {
    return CheckpointingUtils.getLastCheckpointedSuperstep(getFs(),
        savedCheckpointBasePath);
  }

  @Override
  public JobProgressTracker getJobProgressTracker() {
    return getGraphTaskManager().getJobProgressTracker();
  }


  /**
   * For every worker this method returns unique number
   * between 0 and N, where N is the total number of workers.
   * This number stays the same throughout the computation.
   * TaskID may be different from this number and task ID
   * is not necessarily continuous
   * @param workerInfo worker info object
   * @return worker number
   */
  protected int getWorkerId(WorkerInfo workerInfo) {
    return getWorkerInfoList().indexOf(workerInfo);
  }

  /**
   * Returns worker info corresponding to specified worker id.
   * @param id unique worker id
   * @return WorkerInfo
   */
  protected WorkerInfo getWorkerInfoById(int id) {
    return getWorkerInfoList().get(id);
  }
}
