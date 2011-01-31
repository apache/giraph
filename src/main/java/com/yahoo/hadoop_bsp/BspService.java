package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONArray;
import org.json.JSONObject;

import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 * @author aching
 *
 */
@SuppressWarnings("rawtypes")
public class BspService <
    I extends WritableComparable, V extends Writable, E extends Writable,
    M extends Writable> implements Watcher {
    /** Private ZooKeeper instance that implements the service */
    private final ZooKeeperExt m_zk;
    /** Has worker registration changed (either healthy or unhealthy) */
    private final BspEvent m_workerHealthRegistrationChanged;
    /** InputSplits are ready for consumption by workers */
    private final BspEvent m_inputSplitsAllReadyChanged;
    /** InputSplit reservation or finished notification and synchronization */
    private final BspEvent m_inputSplitsStateChanged;
    /** Are the worker assignments of vertex ranges ready? */
    private final BspEvent m_vertexRangeAssignmentsReadyChanged;
    /** Superstep finished synchronization */
    private final BspEvent m_superstepFinished;
    /** Superstep workers finished changed synchronization */
    private final BspEvent m_superstepWorkersFinishedChanged;
    /** Master election changed for any waited on attempt */
    private final BspEvent m_masterElectionChildrenChanged;
    /** Cleaned up directory children changed*/
    private final BspEvent m_cleanedUpChildrenChanged;
    /** Total mappers for this job */
    private final int m_totalMappers;
    /** Configuration of the job*/
    private final Configuration m_conf;
    /** Job context (mainly for progress) */
    private final Context m_context;
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
    /** The current known job state */
    private State m_currentJobState = State.UNKNOWN;
    /** File system */
    private final FileSystem m_fs;
    /** Restarted manually from a checkpoint? */
    private long m_manualRestartSuperstep = -1;
    /** Vertex class */
    private final Class<? extends HadoopVertex<I, V, E, M>> m_hadoopVertexClass;
    /**
     * Used to instantiate messages and call pre/post application/superstep
     * methods
     */
    private final HadoopVertex<I, V, E, M> m_instantiableHadoopVertex;
    /** Used to instantiate vertex ids, vertex values, and edge values */
    private final VertexReader<I, V, E> m_instantiableVertexReader;
    /** Checkpoint frequency */
    int m_checkpointFrequency = -1;
    /** Vertex range set based on the superstep below */
    private NavigableSet<VertexRange<I>> m_vertexRangeSet = null;
    /** Vertex range set is based on this superstep */
    private long m_vertexRangeSuperstep = -1;

    /** State of the application */
    public enum State {
        UNKNOWN,
        INIT,
        RUNNING,
        FAILED,
        FINISHED
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
    public static final String AGGREGATOR_NAME_KEY = "_aggregatorNameKey";
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

    /**
     * Generate the base master election directory path for a given application
     * attempt
     *
     * @param attempt application attempt number
     * @return directory path based on the an attempt
     */
    final public String getMasterElectionPath(long attempt) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt + MASTER_ELECTION_DIR;
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
    public final String CHECKPOINT_FINALIZED_POSTFIX = ".finalized";

    /**
     * Get the ZooKeeperExt instance.
     *
     * @return ZooKeeperExt instance.
     */
    final public ZooKeeperExt getZkExt() {
        return m_zk;
    }

    /**
     * Get the manually restart superstep
     *
     * @return -1 if not manually restarted, otherwise the superstep id
     */
    final public long getManualRestartSuperstep() {
        return m_manualRestartSuperstep;
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

    final public Context getContext() {
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

    final public int getTotalMappers() {
        return m_totalMappers;
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

    final public BspEvent getSuperstepFinishedEvent() {
        return m_superstepFinished;
    }

    final public BspEvent getSuperstepWorkersFinishedChangedEvent() {
        return m_superstepWorkersFinishedChanged;
    }

    final public BspEvent getMasterElectionChildrenChangedEvent() {
        return m_masterElectionChildrenChanged;
    }

    final public BspEvent getCleanedUpChildrenChangedEvent() {
        return m_cleanedUpChildrenChanged;
    }

    final public State getJobState() {
        synchronized(this) {
            if (m_currentJobState == State.UNKNOWN) {
                try {
                    getZkExt().createExt(MASTER_JOB_STATE_PATH,
                                         State.UNKNOWN.toString().getBytes(),
                                         Ids.OPEN_ACL_UNSAFE,
                                         CreateMode.PERSISTENT,
                                         true);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.info("getJobState: Job state already exists (" +
                             MASTER_JOB_STATE_PATH + ")");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                String jobState = new String(
                    m_zk.getData(MASTER_JOB_STATE_PATH, true, null));
                m_currentJobState = State.valueOf(jobState);
            } catch (KeeperException.NoNodeException e) {
                LOG.info("getJobState: Job state path is empty! - " +
                         MASTER_JOB_STATE_PATH);
                m_currentJobState = State.UNKNOWN;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return m_currentJobState;
        }
    }

    public BspService(String serverPortList,
                      int sessionMsecTimeout,
                      Context context,
                      BspJob.BspMapper<I, V, E, M> bspMapper) {
        m_context = context;
        m_bspMapper = bspMapper;
        m_conf = context.getConfiguration();
        m_jobId = m_conf.get("mapred.job.id", "Unknown Job");
        m_taskPartition = m_conf.getInt("mapred.task.partition", -1);
        m_totalMappers = m_conf.getInt("mapred.map.tasks", -1);
        m_workerHealthRegistrationChanged = new ContextLock(m_context);
        m_inputSplitsAllReadyChanged = new ContextLock(m_context);
        m_inputSplitsStateChanged = new ContextLock(m_context);
        m_vertexRangeAssignmentsReadyChanged = new ContextLock(m_context);
        m_superstepFinished = new ContextLock(m_context);
        m_superstepWorkersFinishedChanged = new ContextLock(m_context);
        m_masterElectionChildrenChanged = new ContextLock(m_context);
        m_cleanedUpChildrenChanged = new ContextLock(m_context);
        m_manualRestartSuperstep =
            m_conf.getLong(BspJob.BSP_RESTART_SUPERSTEP, -1);
        m_cachedSuperstep = m_manualRestartSuperstep;
        try {
            m_hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        m_hostnamePartitionId = m_hostname + "_" + getTaskPartition();

        @SuppressWarnings({ "unchecked" })
        Class<? extends HadoopVertex<I, V, E, M>> hadoopVertexClass =
            (Class<? extends HadoopVertex<I, V, E, M>>)
                getConfiguration().getClass(BspJob.BSP_VERTEX_CLASS,
                                            HadoopVertex.class,
                                            HadoopVertex.class);
        m_hadoopVertexClass = hadoopVertexClass;
        @SuppressWarnings({ "unchecked" })
        Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
                (Class<? extends VertexInputFormat<I, V, E>>)
                    getConfiguration().getClass(
                        BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                        VertexInputFormat.class,
                        VertexInputFormat.class);
        try {
            m_instantiableHadoopVertex =
                hadoopVertexClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                "BspService: Couldn't instantiate vertex");
        }
        try {
            m_instantiableVertexReader =
                vertexInputFormatClass.newInstance().createVertexReader(
                    null, getContext());
        } catch (Exception e) {
            throw new RuntimeException(
                "BspService: Couldn't instantiate vertex input format");
        }

        m_checkpointFrequency =
            m_conf.getInt(BspJob.BSP_CHECKPOINT_FREQUENCY,
                          BspJob.DEFAULT_BSP_CHECKPOINT_FREQUENCY);

        BASE_PATH = BASE_DIR + "/" + m_jobId;
        MASTER_JOB_STATE_PATH = BASE_PATH + MASTER_JOB_STATE_NODE;
        INPUT_SPLIT_PATH = BASE_PATH + INPUT_SPLIT_DIR;
        INPUT_SPLITS_ALL_READY_PATH = BASE_PATH + INPUT_SPLITS_ALL_READY_NODE;
        APPLICATION_ATTEMPTS_PATH = BASE_PATH + APPLICATION_ATTEMPTS_DIR;
        CLEANED_UP_PATH = BASE_PATH + CLEANED_UP_DIR;
        CHECKPOINT_BASE_PATH =
            getConfiguration().get(
                BspJob.BSP_CHECKPOINT_DIRECTORY,
                BspJob.DEFAULT_BSP_CHECKPOINT_DIRECTORY + "/" + getJobId());
        LOG.info("BspService: Connecting to ZooKeeper with job " + m_jobId +
                 ", " + getTaskPartition() + " on " + serverPortList);
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
     * Instantiate a vertex index
     *
     * @return new vertex index
     */
    final public I createVertexIndex() {
        return m_instantiableVertexReader.createVertexId();
    }

    /**
     * Instantiate a vertex value
     *
     * @return new vertex value
     */
    final public V createVertexValue() {
        return m_instantiableVertexReader.createVertexValue();
    }

    /**
     * Instantiate an edge value
     *
     * @return new edge value
     */
    final public E createEdgeValue() {
        return m_instantiableVertexReader.createEdgeValue();
    }

    /**
     * Instantiate an message value
     *
     * @return new message value
     */
    final public M createMsgValue() {
        return m_instantiableHadoopVertex.createMsgValue();
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
            LOG.info("getApplicationAttempt: Node " +
                     APPLICATION_ATTEMPTS_PATH + " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            List<String> superstepList =
                m_zk.getChildrenExt(superstepPath, true, false, false);
            if (superstepList.isEmpty()) {
                m_cachedSuperstep = 0;
            }
            else {
                m_cachedSuperstep =
                    Long.parseLong(Collections.max(superstepList));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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

     public NavigableSet<VertexRange<I>> getVertexRangeSet(long superstep) {
         if (m_vertexRangeSuperstep == superstep) {
             return m_vertexRangeSet;
         }

         m_vertexRangeSet = new TreeSet<VertexRange<I>>();
         String vertexRangeAssignmentsPath =
             getVertexRangeAssignmentsPath(getApplicationAttempt(),
                                           superstep);
         try {
             JSONArray vertexRangeAssignmentsArray =
                 new JSONArray(
                     new String(
                         getZkExt().getData(vertexRangeAssignmentsPath,
                                            false,
                                            null)));
             LOG.debug("getVertexRangeSet: Found vertex ranges " +
                       vertexRangeAssignmentsArray.toString() +
                       " on superstep " + superstep);
             for (int i = 0; i < vertexRangeAssignmentsArray.length(); ++i) {
                 JSONObject vertexRangeObj =
                     vertexRangeAssignmentsArray.getJSONObject(i);
                 I maxVertexIndex = createVertexIndex();
                 VertexRange<I> vertexRange =
                     new VertexRange<I>(maxVertexIndex, vertexRangeObj);
                 m_vertexRangeSet.add(vertexRange);
             }
         } catch (Exception e) {
             throw new RuntimeException(e);
         }

         return m_vertexRangeSet;
     }

     /**
      * Default reaction to a change event in workerHealthRegistration.
      */
     protected void workerHealthRegistrationChanged(String Path) {
         m_workerHealthRegistrationChanged.signal();
     }

    final public void process(WatchedEvent event) {
        LOG.info("process: Got a new event, path = " + event.getPath() +
                 ", type = " + event.getType() + ", state = " +
                 event.getState());

        if (event.getPath() == null) {
            // No way to recover from a disconnect event
            if ((event.getType() == EventType.None) &&
                (event.getState() == KeeperState.Disconnected)) {
                m_inputSplitsAllReadyChanged.signal();
                m_inputSplitsStateChanged.signal();
                m_superstepFinished.signal();
                m_superstepWorkersFinishedChanged.signal();
                m_masterElectionChildrenChanged.signal();
                throw new RuntimeException(
                    "process: Disconnected from ZooKeeper, cannot recover.");
            }
            return;
        }

        if (event.getPath().equals(MASTER_JOB_STATE_PATH)) {
            synchronized (this) {
                try {
                    String jobState =
                        new String(
                            m_zk.getData(MASTER_JOB_STATE_PATH, true, null));
                    m_currentJobState = State.valueOf(jobState);
                } catch (KeeperException.NoNodeException e) {
                    LOG.info("process: Job state path is empty! - " +
                             MASTER_JOB_STATE_PATH);
                    m_currentJobState = State.UNKNOWN;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            // This will cause all becomeMaster() MasterThreads to notice the
            // change in job state and quit trying to become the master.
            m_masterElectionChildrenChanged.signal();
        }
        else if ((event.getPath().contains(WORKER_HEALTHY_DIR) ||
                 event.getPath().contains(WORKER_UNHEALTHY_DIR)) &&
                 (event.getType() == EventType.NodeChildrenChanged)) {
            LOG.info("process: m_workerHealthRegistrationChanged " +
                     "(worker health reported)");
            workerHealthRegistrationChanged(event.getPath());
        }
        else if (event.getPath().equals(INPUT_SPLITS_ALL_READY_PATH) &&
                (event.getType() == EventType.NodeCreated)) {
           LOG.info("process: m_inputSplitsReadyChanged (input splits ready)");
           m_inputSplitsAllReadyChanged.signal();
        }
        else if (event.getPath().endsWith(INPUT_SPLIT_RESERVED_NODE) &&
                (event.getType() == EventType.NodeDeleted)) {
           LOG.info("process: m_inputSplitsStateChanged (lost a reservation)");
           m_inputSplitsStateChanged.signal();
        }
        else if (event.getPath().endsWith(INPUT_SPLIT_FINISHED_NODE) &&
                (event.getType() == EventType.NodeCreated)) {
           LOG.info("process: m_inputSplitsStateChanged (finished inputsplit)");
           m_inputSplitsStateChanged.signal();
        }
        else if (event.getPath().contains(VERTEX_RANGE_ASSIGNMENTS_DIR) &&
                event.getType() == EventType.NodeCreated) {
           LOG.info("process: m_vertexRangeAssignmentsReadyChanged signaled");
           m_vertexRangeAssignmentsReadyChanged.signal();
       }
        else if (event.getPath().contains(SUPERSTEP_FINISHED_NODE) &&
                event.getType() == EventType.NodeCreated) {
           LOG.info("process: m_superstepFinished signaled");
           m_superstepFinished.signal();
       }
        else if (event.getPath().contains(WORKER_FINISHED_DIR) &&
                 event.getType() == EventType.NodeChildrenChanged) {
           LOG.info("process: m_superstepWorkersFinished signaled");
           m_superstepWorkersFinishedChanged.signal();
       }
        else if (event.getPath().contains(MASTER_ELECTION_DIR) &&
                 event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_masterElectionChildrenChanged signaled");
            m_masterElectionChildrenChanged.signal();
        }
        else if (event.getPath().equals(CLEANED_UP_PATH) &&
                 event.getType() == EventType.NodeChildrenChanged) {
            LOG.info("process: m_cleanedUpChildrenChanged signaled");
            m_cleanedUpChildrenChanged.signal();
        }
        else {
            LOG.warn("process: Unknown event");
        }
    }
}
