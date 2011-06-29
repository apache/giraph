package org.apache.giraph;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.giraph.BspJob.BspMapper;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 */
@SuppressWarnings("rawtypes")
public abstract class BspService <
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        implements Watcher, CentralizedService<I, V, E, M> {
    /** Private ZooKeeper instance that implements the service */
    private final ZooKeeperExt m_zk;
    /** Has worker registration changed (either healthy or unhealthy) */
    private final BspEvent m_workerHealthRegistrationChanged =
        new PredicateLock();
    /** InputSplits are ready for consumption by workers */
    private final BspEvent m_inputSplitsAllReadyChanged =
        new PredicateLock();
    /** InputSplit reservation or finished notification and synchronization */
    private final BspEvent m_inputSplitsStateChanged =
        new PredicateLock();
    /** Are the worker assignments of vertex ranges ready? */
    private final BspEvent m_vertexRangeAssignmentsReadyChanged =
        new PredicateLock();
    /** Have the vertex range exchange children changed? */
    private final BspEvent m_vertexRangeExchangeChildrenChanged =
        new PredicateLock();
    /** Are the vertex range exchanges done? */
    private final BspEvent m_vertexRangeExchangeFinishedChanged =
        new PredicateLock();
    /** Application attempt changed */
    private final BspEvent m_applicationAttemptChanged =
        new PredicateLock();
    /** Superstep finished synchronization */
    private final BspEvent m_superstepFinished =
        new PredicateLock();
    /** Master election changed for any waited on attempt */
    private final BspEvent m_masterElectionChildrenChanged =
        new PredicateLock();
    /** Cleaned up directory children changed*/
    private final BspEvent m_cleanedUpChildrenChanged =
        new PredicateLock();
    /** Registered list of BspEvents */
    private final List<BspEvent> m_registeredBspEvents =
        new ArrayList<BspEvent>();
    /** Configuration of the job*/
    private final Configuration m_conf;
    /** Job context (mainly for progress) */
    private final Mapper<?, ?, ?, ?>.Context m_context;
    /** Cached superstep (from ZooKeeper) */
    private long m_cachedSuperstep = -1;
    /** Cached application attempt (from ZooKeeper) */
    private long m_cachedApplicationAttempt = -1;
    /** Job id, to ensure uniqueness */
    private final String m_jobId;
    /** Task partition, to ensure uniqueness */
    private final int m_taskPartition;
    /** My hostname */
    private final String m_hostname;
    /** Combination of hostname '_' partition (unique id) */
    private final String m_hostnamePartitionId;
    /** Mapper that will do computation */
    private final BspJob.BspMapper<I, V, E, M> m_bspMapper;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspService.class);
    /** File system */
    private final FileSystem m_fs;
    /** Restarted from a checkpoint (manual or automatic) */
    private long m_restartedSuperstep = -1;
    /** Vertex class */
    private final Class<? extends HadoopVertex<I, V, E, M>> m_hadoopVertexClass;
    /**
     * Used to instantiate messages and call pre/post application/superstep
     * methods
     */
    private final HadoopVertex<I, V, E, M> m_instantiableHadoopVertex;
    /** Checkpoint frequency */
    private int m_checkpointFrequency = -1;
    /** Vertex range map based on the superstep below */
    private NavigableMap<I, VertexRange<I, V, E, M>> m_vertexRangeMap =
        new TreeMap<I, VertexRange<I, V, E, M>>();
    /** Vertex range set is based on this superstep */
    private long m_vertexRangeSuperstep = -1;
    /** Map of aggregators */
    private Map<String, Aggregator<Writable>> m_aggregatorMap =
        new TreeMap<String, Aggregator<Writable>>();

    /** State of the application */
    public enum State {
        UNKNOWN, ///< Shouldn't be seen, just an initial state
        START_SUPERSTEP, ///< Start from a desired superstep
        FAILED, ///< Unrecoverable
        FINISHED ///< Successful completion
    }

    public static final String BASE_DIR = "/_hadoopBsp";
    public static final String MASTER_JOB_STATE_NODE = "/_masterJobState";
    public static final String INPUT_SPLIT_DIR = "/_inputSplitsDir";
    public static final String INPUT_SPLIT_RESERVED_NODE =
        "/_inputSplitReserved";
    public static final String INPUT_SPLIT_FINISHED_NODE =
        "/_inputSplitFinished";
    public static final String INPUT_SPLITS_ALL_READY_NODE =
        "/_inputSplitsAllReady";
    public static final String APPLICATION_ATTEMPTS_DIR =
        "/_applicationAttemptsDir";
    public static final String MASTER_ELECTION_DIR = "/_masterElectionDir";
    public static final String SUPERSTEP_DIR = "/_superstepDir";
    public static final String MERGED_AGGREGATOR_DIR =
        "/_mergedAggregatorDir";
    public static final String WORKER_HEALTHY_DIR = "/_workerHealthyDir";
    public static final String WORKER_UNHEALTHY_DIR = "/_workerUnhealthyDir";
    public static final String WORKER_FINISHED_DIR = "/_workerFinishedDir";
    public static final String VERTEX_RANGE_ASSIGNMENTS_DIR =
        "/_vertexRangeAssignments";
    public static final String VERTEX_RANGE_EXCHANGE_DIR =
        "/_vertexRangeExchangeDir";
    public static final String VERTEX_RANGE_EXCHANGED_FINISHED_NODE =
        "/_vertexRangeExchangeFinished";
    public static final String SUPERSTEP_FINISHED_NODE = "/_superstepFinished";
    public static final String CLEANED_UP_DIR = "/_cleanedUpDir";

    public static final String JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY =
        "_aggregatorValueArrayKey";
    public static final String JSONOBJ_VERTEX_RANGE_STAT_ARRAY_KEY =
        "_vertexRangeStatArrayKey";
    public static final String JSONOBJ_FINISHED_VERTICES_KEY =
        "_verticesFinishedKey";
    public static final String JSONOBJ_NUM_VERTICES_KEY = "_numVerticesKey";
    public static final String JSONOBJ_NUM_EDGES_KEY = "_numEdgesKey";
    public static final String JSONOBJ_HOSTNAME_ID_KEY = "_hostnameIdKey";
    public static final String JSONOBJ_MAX_VERTEX_INDEX_KEY =
        "_maxVertexIndexKey";
    public static final String JSONOBJ_HOSTNAME_KEY = "_hostnameKey";
    public static final String JSONOBJ_PORT_KEY = "_portKey";
    public static final String JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY =
        "_checkpointFilePrefixKey";
    public static final String JSONOBJ_PREVIOUS_HOSTNAME_KEY =
        "_previousHostnameKey";
    public static final String JSONOBJ_PREVIOUS_PORT_KEY = "_previousPortKey";
    public static final String JSONOBJ_STATE_KEY = "_stateKey";
    public static final String JSONOBJ_APPLICATION_ATTEMPT_KEY =
        "_applicationAttemptKey";
    public static final String JSONOBJ_SUPERSTEP_KEY =
        "_superstepKey";
    public static final String AGGREGATOR_NAME_KEY = "_aggregatorNameKey";
    public static final String AGGREGATOR_CLASS_NAME_KEY =
        "_aggregatorClassNameKey";
    public static final String AGGREGATOR_VALUE_KEY = "_aggregatorValueKey";

    public static final String WORKER_SUFFIX = "_worker";
    public static final String MASTER_SUFFIX = "_master";

    /** Path to the job's root */
    public final String BASE_PATH;
    /** Path to the job state determined by the master (informative only) */
    public final String MASTER_JOB_STATE_PATH;
    /** Path to the input splits written by the master */
    public final String INPUT_SPLIT_PATH;
    /** Path to the input splits all ready to be processed by workers */
    public final String INPUT_SPLITS_ALL_READY_PATH;
    /** Path to the application attempts) */
    public final String APPLICATION_ATTEMPTS_PATH;
    /** Path to the cleaned up notifications */
    public final String CLEANED_UP_PATH;
    /** Path to the checkpoint's root (including job id) */
    public final String CHECKPOINT_BASE_PATH;
    /** Path to the master election path */
    public final String MASTER_ELECTION_PATH;

    /**
     * Get the superstep from a ZooKeeper path
     *
     * @param path Path to parse for the superstep
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("getSuperstepFromPath: Got path=" + path +
                      ", start=" + foundSuperstepStart + ", end=" + endIndex);
        }
        return Long.parseLong(path.substring(foundSuperstepStart, endIndex));
    }

    /**
     * Get the hostname and id from a "healthy" worker path
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
    final public String getSuperstepPath(long attempt) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt + SUPERSTEP_DIR;
    }

    /**
     * Generate the worker "healthy" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getWorkerHealthyPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_HEALTHY_DIR;
    }

    /**
     * Generate the worker "unhealthy" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getWorkerUnhealthyPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_UNHEALTHY_DIR;
    }

    /**
     * Generate the worker "finished" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getWorkerFinishedPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_FINISHED_DIR;
    }

    /**
     * Generate the "vertex range assignments" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getVertexRangeAssignmentsPath(long attempt,
                                                      long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + VERTEX_RANGE_ASSIGNMENTS_DIR;
    }

    /**
     * Generate the "vertex range exchange" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getVertexRangeExchangePath(long attempt,
                                                   long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + VERTEX_RANGE_EXCHANGE_DIR;
    }

    /**
     * Generate the "vertex range exchange finished" directory path for
     * a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getVertexRangeExchangeFinishedPath(long attempt,
                                                           long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep +
            VERTEX_RANGE_EXCHANGED_FINISHED_NODE;
    }

    /**
     * Generate the merged aggregator directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getMergedAggregatorPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + MERGED_AGGREGATOR_DIR;
    }

    /**
     * Generate the "superstep finished" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getSuperstepFinishedPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + SUPERSTEP_FINISHED_NODE;
    }

    /**
     * Generate the base superstep directory path for a given application
     * attempt
     *
     * @param attempt application attempt number
     * @return directory path based on the an attempt
     */
    final public String getCheckpointBasePath(long superstep) {
        return CHECKPOINT_BASE_PATH + "/" + superstep;
    }

    /** If at the end of a checkpoint file, indicates metadata */
    public final String CHECKPOINT_METADATA_POSTFIX = ".metadata";

    /**
     * If at the end of a checkpoint file, indicates vertices, edges,
     * messages, etc.
     */
    public final String CHECKPOINT_VERTICES_POSTFIX = ".vertices";

    /**
     * If at the end of a checkpoint file, indicates metadata and data is valid
     * for the same filenames without .valid
     */
    public final String CHECKPOINT_VALID_POSTFIX = ".valid";

    /**
     * If at the end of a checkpoint file, indicates the stitched checkpoint
     * file prefixes.  A checkpoint is not valid if this file does not exist.
     */
    public static final String CHECKPOINT_FINALIZED_POSTFIX = ".finalized";

    /**
     * Get the checkpoint from a finalized checkpoint path
     *
     * @param path Path contain
     * @return checkpoint of the finalized path
     */
    public static long getCheckpoint(Path finalizedPath) {
        if (!finalizedPath.getName().endsWith(CHECKPOINT_FINALIZED_POSTFIX)) {
            throw new InvalidParameterException(
                "getCheckpoint: " + finalizedPath + "Doesn't end in " +
                CHECKPOINT_FINALIZED_POSTFIX);
        }
        String checkpointString =
            finalizedPath.getName().replace(CHECKPOINT_FINALIZED_POSTFIX, "");
        return Long.parseLong(checkpointString);
    }

    /**
     * Get the ZooKeeperExt instance.
     *
     * @return ZooKeeperExt instance.
     */
    final public ZooKeeperExt getZkExt() {
        return m_zk;
    }

    @Override
    final public long getRestartedSuperstep() {
        return m_restartedSuperstep;
    }

    /**
     * Set the restarted superstep
     *
     * @param superstep Set the manually restarted superstep
     */
    final public void setRestartedSuperstep(long superstep) {
        m_restartedSuperstep= superstep;
    }

    final public boolean checkpointFrequencyMet(long superstep) {
        if ((superstep == 1) ||
            (((superstep + 1) % m_checkpointFrequency) == 0)) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Get the file system
     *
     * @return file system
     */
    final public FileSystem getFs() {
        return m_fs;
    }

    final public Configuration getConfiguration() {
        return m_conf;
    }

    final public Mapper<?, ?, ?, ?>.Context getContext() {
        return m_context;
    }

    final public String getHostname() {
        return m_hostname;
    }

    final public String getHostnamePartitionId() {
        return m_hostnamePartitionId;
    }

    final public int getTaskPartition() {
        return m_taskPartition;
    }

    final public BspMapper<I, V, E, M> getBspMapper() {
        return m_bspMapper;
    }

    final public BspEvent getWorkerHealthRegistrationChangedEvent() {
        return m_workerHealthRegistrationChanged;
    }

    final public BspEvent getInputSplitsAllReadyEvent() {
        return m_inputSplitsAllReadyChanged;
    }

    final public BspEvent getInputSplitsStateChangedEvent() {
        return m_inputSplitsStateChanged;
    }

    final public BspEvent getVertexRangeAssignmentsReadyChangedEvent() {
        return m_vertexRangeAssignmentsReadyChanged;
    }

    final public BspEvent getVertexRangeExchangeChildrenChangedEvent() {
        return m_vertexRangeExchangeChildrenChanged;
    }

    final public BspEvent getVertexRangeExchangeFinishedChangedEvent() {
        return m_vertexRangeExchangeFinishedChanged;
    }

    final public BspEvent getApplicationAttemptChangedEvent() {
        return m_applicationAttemptChanged;
    }

    final public BspEvent getSuperstepFinishedEvent() {
        return m_superstepFinished;
    }


    final public BspEvent getMasterElectionChildrenChangedEvent() {
        return m_masterElectionChildrenChanged;
    }

    final public BspEvent getCleanedUpChildrenChangedEvent() {
        return m_cleanedUpChildrenChanged;
    }

    /**
     * Get the master commanded job state as a JSONObject.  Also sets the
     * watches to see if the master commanded job state changes.
     *
     * @return Last job state or null if none
     */
    final public JSONObject getJobState() {
        try {
            getZkExt().createExt(MASTER_JOB_STATE_PATH,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("getJobState: Job state already exists (" +
                     MASTER_JOB_STATE_PATH + ")");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String jobState = null;
        try {
            List<String> childList =
                m_zk.getChildrenExt(MASTER_JOB_STATE_PATH, true, true, true);
            if (childList.isEmpty()) {
                return null;
            }
            jobState =
                new String(m_zk.getData(childList.get(childList.size() - 1), true, null));
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getJobState: Job state path is empty! - " +
                     MASTER_JOB_STATE_PATH);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            return new JSONObject(jobState);
        } catch (JSONException e) {
            throw new RuntimeException(
                "getJobState: Failed to parse job state " + jobState);
        }
    }

    public BspService(String serverPortList,
                      int sessionMsecTimeout,
                      Mapper<?, ?, ?, ?>.Context context,
                      BspJob.BspMapper<I, V, E, M> bspMapper) {
        registerBspEvent(m_workerHealthRegistrationChanged);
        registerBspEvent(m_inputSplitsAllReadyChanged);
        registerBspEvent(m_inputSplitsStateChanged);
        registerBspEvent(m_vertexRangeAssignmentsReadyChanged);
        registerBspEvent(m_vertexRangeExchangeChildrenChanged);
        registerBspEvent(m_vertexRangeExchangeFinishedChanged);
        registerBspEvent(m_applicationAttemptChanged);
        registerBspEvent(m_superstepFinished);
        registerBspEvent(m_masterElectionChildrenChanged);
        registerBspEvent(m_cleanedUpChildrenChanged);

        m_context = context;
        m_bspMapper = bspMapper;
        m_conf = context.getConfiguration();
        m_jobId = m_conf.get("mapred.job.id", "Unknown Job");
        m_taskPartition = m_conf.getInt("mapred.task.partition", -1);
        m_restartedSuperstep =
            m_conf.getLong(BspJob.RESTART_SUPERSTEP, -1);
        m_cachedSuperstep = m_restartedSuperstep;
        try {
            m_hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        m_hostnamePartitionId = m_hostname + "_" + getTaskPartition();

        m_hadoopVertexClass = BspUtils.getVertexClass(getConfiguration());
        m_instantiableHadoopVertex =
            BspUtils.<I, V, E, M>createVertex(getConfiguration());

        m_checkpointFrequency =
            m_conf.getInt(BspJob.CHECKPOINT_FREQUENCY,
                          BspJob.CHECKPOINT_FREQUENCY_DEFAULT);

        BASE_PATH = BASE_DIR + "/" + m_jobId;
        MASTER_JOB_STATE_PATH = BASE_PATH + MASTER_JOB_STATE_NODE;
        INPUT_SPLIT_PATH = BASE_PATH + INPUT_SPLIT_DIR;
        INPUT_SPLITS_ALL_READY_PATH = BASE_PATH + INPUT_SPLITS_ALL_READY_NODE;
        APPLICATION_ATTEMPTS_PATH = BASE_PATH + APPLICATION_ATTEMPTS_DIR;
        CLEANED_UP_PATH = BASE_PATH + CLEANED_UP_DIR;
        CHECKPOINT_BASE_PATH =
            getConfiguration().get(
                BspJob.CHECKPOINT_DIRECTORY,
                BspJob.CHECKPOINT_DIRECTORY_DEFAULT + "/" + getJobId());
        MASTER_ELECTION_PATH = BASE_PATH + MASTER_ELECTION_DIR;
        if (LOG.isInfoEnabled()) {
            LOG.info("BspService: Connecting to ZooKeeper with job " + m_jobId +
                     ", " + getTaskPartition() + " on " + serverPortList);
        }
        try {
            m_zk = new ZooKeeperExt(serverPortList, sessionMsecTimeout, this);
            m_fs = FileSystem.get(getConfiguration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the job id
     *
     * @return job id
     */
    final public String getJobId() {
        return m_jobId;
    }

    /**
     * Get the hadoop vertex class (mainly for instantiation)
     *
     * @return the hadoop vertex class
     */
    final public Class<? extends HadoopVertex<I, V, E, M>> getHadoopVertexClass() {
        return m_hadoopVertexClass;
    }

    final public Vertex<I, V, E, M> getRepresentativeVertex() {
        return m_instantiableHadoopVertex;
    }

    /**
     * Get the latest application attempt and cache it.
     *
     * @return the latest application attempt
     */
    final public long getApplicationAttempt() {
        if (m_cachedApplicationAttempt != -1) {
            return m_cachedApplicationAttempt;
        }
        try {
            m_zk.createExt(APPLICATION_ATTEMPTS_PATH,
                           null,
                           Ids.OPEN_ACL_UNSAFE,
                           CreateMode.PERSISTENT,
                           true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("getApplicationAttempt: Node " +
                     APPLICATION_ATTEMPTS_PATH + " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            List<String> attemptList =
                m_zk.getChildrenExt(
                    APPLICATION_ATTEMPTS_PATH, true, false, false);
            if (attemptList.isEmpty()) {
                m_cachedApplicationAttempt = 0;
            }
            else {
                m_cachedApplicationAttempt =
                    Long.parseLong(Collections.max(attemptList));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return m_cachedApplicationAttempt;
    }

    /**
     * Get the latest superstep and cache it.
     *
     * @return the latest superstep
     * @throws InterruptedException
     * @throws KeeperException
     */
    final public long getSuperstep() {
        if (m_cachedSuperstep != -1) {
            return m_cachedSuperstep;
        }
        String superstepPath = getSuperstepPath(getApplicationAttempt());
        try {
            m_zk.createExt(superstepPath,
                           null,
                           Ids.OPEN_ACL_UNSAFE,
                           CreateMode.PERSISTENT,
                           true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("getApplicationAttempt: Node " +
                         APPLICATION_ATTEMPTS_PATH + " already exists!");
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
            superstepList = m_zk.getChildrenExt(superstepPath, true, false, false);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "getSuperstep: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "getSuperstep: InterruptedException", e);
        }
        if (superstepList.isEmpty()) {
            m_cachedSuperstep = 0;
        }
        else {
            m_cachedSuperstep =
                Long.parseLong(Collections.max(superstepList));
        }

        return m_cachedSuperstep;
    }

    /**
     * Increment the cached superstep.
     */
    final public void incrCachedSuperstep() {
        if (m_cachedSuperstep == -1) {
            throw new RuntimeException("incrSuperstep: Invalid -1 superstep.");
        }
        ++m_cachedSuperstep;
    }

    /**
     * Set the cached superstep (should only be used for loading checkpoints
     * or recovering from failure).
     *
     * @param superstep will be used as the next superstep iteration
     */
    final public void setCachedSuperstep(long superstep) {
        m_cachedSuperstep = superstep;
    }

    /**
     * Set the cached application attempt (should only be used for restart from
     * failure by the master)
     *
     * @param applicationAttempt Will denote the new application attempt
     */
    final public void setApplicationAttempt(long applicationAttempt) {
        m_cachedApplicationAttempt = applicationAttempt;
        String superstepPath = getSuperstepPath(m_cachedApplicationAttempt);
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

    public NavigableMap<I, VertexRange<I, V, E, M>> getVertexRangeMap(
            long superstep) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getVertexRangeMap: Current superstep = " +
                      getSuperstep() + ", desired superstep = " + superstep);
        }
        // The master will try to get superstep 0 and need to be refreshed
        // The worker will try to get superstep 0 and needs to get its value
        if (superstep == 0) {
            if (getSuperstep() == 1) {
                // Master should refresh
            }
            else if (getSuperstep() == 0) {
                // Worker will populate
                return m_vertexRangeMap;
            }
        }
        else if (m_vertexRangeSuperstep == superstep) {
            return m_vertexRangeMap;
        }

        m_vertexRangeSuperstep = superstep;
        NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap =
            new TreeMap<I, VertexRange<I, V, E, M>>();
        String vertexRangeAssignmentsPath =
            getVertexRangeAssignmentsPath(getApplicationAttempt(),
                                          superstep);
        try {
            JSONArray vertexRangeAssignmentsArray =
                new JSONArray(
                    new String(getZkExt().getData(vertexRangeAssignmentsPath,
                                                  false,
                                                  null)));
            LOG.debug("getVertexRangeSet: Found vertex ranges " +
                      vertexRangeAssignmentsArray.toString() +
                      " on superstep " + superstep);
            for (int i = 0; i < vertexRangeAssignmentsArray.length(); ++i) {
                JSONObject vertexRangeObj =
                    vertexRangeAssignmentsArray.getJSONObject(i);
                Class<I> indexClass =
                    BspUtils.getVertexIndexClass(getConfiguration());
                VertexRange<I, V, E, M> vertexRange =
                    new VertexRange<I, V, E, M>(indexClass,
                            vertexRangeObj);
                if (vertexRangeMap.containsKey(vertexRange.getMaxIndex())) {
                    throw new RuntimeException(
                        "getVertexRangeMap: Impossible that vertex range " +
                        "max " + vertexRange.getMaxIndex() +
                        " already exists!");
                }
                vertexRangeMap.put(vertexRange.getMaxIndex(), vertexRange);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Copy over the vertices to the vertex ranges
        for (Entry<I, VertexRange<I, V, E, M>> entry :
            vertexRangeMap.entrySet()) {
            if (!m_vertexRangeMap.containsKey(entry.getKey())) {
                continue;
            }
            VertexRange<I, V, E, M> vertexRange =
                m_vertexRangeMap.get(entry.getKey());
            entry.getValue().getVertexMap().putAll(
                vertexRange.getVertexMap());
        }
        m_vertexRangeMap = vertexRangeMap;
        return m_vertexRangeMap;
    }

    public NavigableMap<I, VertexRange<I, V, E, M>> getCurrentVertexRangeMap()
    {
        return m_vertexRangeMap;
    }

    /**
     * Register an aggregator with name.
     *
     * @param name
     * @param aggregator
     * @return boolean (false when aggregator already registered)
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        if (m_aggregatorMap.get(name) != null) {
            return null;
        }
        Aggregator<A> aggregator =
            (Aggregator<A>) aggregatorClass.newInstance();
        @SuppressWarnings("unchecked")
        Aggregator<Writable> writableAggregator =
            (Aggregator<Writable>) aggregator;
        m_aggregatorMap.put(name, writableAggregator);
        LOG.info("registered aggregator=" + name);
        return aggregator;
    }

    /**
     * Get aggregator by name.
     *
     * @param name
     * @return Aggregator<A> (null when not registered)
     */
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return m_aggregatorMap.get(name);
    }

    /**
     * Get the aggregator map.
     */
    public Map<String, Aggregator<Writable>> getAggregatorMap() {
        return m_aggregatorMap;
    }

    /**
     * Register a BspEvent.  Ensure that it will be signaled
     * by catastrophic failure so that threads waiting on an event signal
     * will be unblocked.
     */
    public void registerBspEvent(BspEvent event) {
        m_registeredBspEvents.add(event);
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
    final public void process(WatchedEvent event) {
        // 1. Process all shared events
        // 2. Process specific derived class events

        if (LOG.isInfoEnabled()) {
            LOG.info("process: Got a new event, path = " + event.getPath() +
                     ", type = " + event.getType() + ", state = " +
                     event.getState());
        }

        if (event.getPath() == null) {
            // No way to recover from a disconnect event, signal all BspEvents
            if ((event.getType() == EventType.None) &&
                    (event.getState() == KeeperState.Disconnected)) {
                for (BspEvent bspEvent : m_registeredBspEvents) {
                    bspEvent.signal();
                }
                throw new RuntimeException(
                    "process: Disconnected from ZooKeeper, cannot recover.");
            }
            return;
        }

        boolean eventProcessed = false;
        if (event.getPath().startsWith(MASTER_JOB_STATE_PATH)) {
            // This will cause all becomeMaster() MasterThreads to notice the
            // change in job state and quit trying to become the master.
            m_masterElectionChildrenChanged.signal();

        } else if ((event.getPath().contains(WORKER_HEALTHY_DIR) ||
                event.getPath().contains(WORKER_UNHEALTHY_DIR)) &&
                (event.getType() == EventType.NodeChildrenChanged)) {
            LOG.info("process: m_workerHealthRegistrationChanged " +
            "(worker health reported - healthy/unhealthy )");
            m_workerHealthRegistrationChanged.signal();
        } else if (event.getPath().equals(INPUT_SPLITS_ALL_READY_PATH) &&
                (event.getType() == EventType.NodeCreated)) {
            LOG.info("process: m_inputSplitsReadyChanged (input splits ready)");
            m_inputSplitsAllReadyChanged.signal();
        } else if (event.getPath().endsWith(INPUT_SPLIT_RESERVED_NODE) &&
                (event.getType() == EventType.NodeDeleted)) {
            LOG.info("process: m_inputSplitsStateChanged (lost a reservation)");
            m_inputSplitsStateChanged.signal();
        } else if (event.getPath().endsWith(INPUT_SPLIT_FINISHED_NODE) &&
                (event.getType() == EventType.NodeCreated)) {
            LOG.info("process: m_inputSplitsStateChanged (finished inputsplit)");
            m_inputSplitsStateChanged.signal();
        } else if (event.getPath().contains(VERTEX_RANGE_ASSIGNMENTS_DIR) &&
                event.getType() == EventType.NodeCreated) {
            LOG.info("process: m_vertexRangeAssignmentsReadyChanged signaled");
            m_vertexRangeAssignmentsReadyChanged.signal();
        } else if (event.getPath().contains(VERTEX_RANGE_EXCHANGE_DIR) &&
                event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_vertexRangeExchangeChildrenChanged signaled");
            m_vertexRangeExchangeChildrenChanged.signal();
        } else if (event.getPath().contains(
                VERTEX_RANGE_EXCHANGED_FINISHED_NODE) &&
                event.getType() == EventType.NodeCreated) {
            LOG.info("process: m_vertexRangeExchangeFinishedChanged signaled");
            m_vertexRangeExchangeFinishedChanged.signal();
        } else if (event.getPath().contains(SUPERSTEP_FINISHED_NODE) &&
                event.getType() == EventType.NodeCreated) {
            LOG.info("process: m_superstepFinished signaled");
            m_superstepFinished.signal();
        } else if (event.getPath().endsWith(APPLICATION_ATTEMPTS_PATH) &&
                event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_applicationAttempChanged signaled");
            m_applicationAttemptChanged.signal();
        } else if (event.getPath().contains(MASTER_ELECTION_DIR) &&
                event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_masterElectionChildrenChanged signaled");
            m_masterElectionChildrenChanged.signal();
        } else if (event.getPath().equals(CLEANED_UP_PATH) &&
                event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_cleanedUpChildrenChanged signaled");
            m_cleanedUpChildrenChanged.signal();
        }
        if ((processEvent(event) == false) && (eventProcessed == false)) {
            LOG.warn("process: Unknown and unprocessed event (path=" +
                     event.getPath() + ", type=" + event.getType() +
                     ", state=" + event.getState() + ")");
        }
    }
}
