package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
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

import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 * @author aching
 *
 */
public class BspService <
    I extends WritableComparable, V, E, M extends Writable> implements Watcher {
    /** Private ZooKeeper instance that implements the service */
    private final ZooKeeperExt m_zk;
    /** Has worker registration changed (either healthy or unhealthy) */
    private final BspEvent m_workerHealthRegistrationChanged;
    /** InputSplits are ready for consumption by workers */
    private final BspEvent m_inputSplitsAllReadyChanged;
    /** InputSplit reservation or finished notification and synchronization */
    private final BspEvent m_inputSplitsStateChanged;
    /** Are the worker assignments of partitions ready? */
    private final BspEvent m_workerPartitionsAllReadyChanged;
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
    @SuppressWarnings("rawtypes")
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
    public static final String VERTEX_RANGE_STATS_DIR = "/_vertexRangesStatsDir";
    public static final String AGGREGATOR_DIR = "/_aggregatorDir";
    public static final String WORKER_HEALTHY_DIR = "/_workerHealthyDir";
    public static final String WORKER_UNHEALTHY_DIR = "/_workerUnhealthyDir";
    public static final String WORKER_SELECTED_DIR = "/_workerSelectedDir";
    public static final String WORKER_SELECTION_FINISHED_NODE =
        "/_workerSelectionFinished";
    public static final String WORKER_FINISHED_DIR = "/_workerFinishedDir";
    public static final String SUPERSTEP_FINISHED_NODE = "/_superstepFinished";
    public static final String CLEANED_UP_DIR = "/_cleanedUpDir";
    public static final String STAT_FINISHED_VERTICES_KEY =
        "_verticesFinishedKey";
    public static final String STAT_NUM_VERTICES_KEY = "_numVerticesKey";
    public static final String STAT_HOSTNAME_ID_KEY = "_hostnameIdKey";
    public static final String STAT_MAX_INDEX_KEY = "_maxIndexKey";
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
     * Generate the worker "selected" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getWorkerSelectedPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_SELECTED_DIR;
    }

    /**
     * Generate the worker "selection finished" directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getWorkerSelectionFinishedPath(long attempt,
                                                       long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_SELECTION_FINISHED_NODE;
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
     * Generate the "vertex range stats" directory path for a chosen worker
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getVertexRangeStatsPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_SELECTED_DIR + "/" +
            getHostnamePartitionId() + VERTEX_RANGE_STATS_DIR;
    }

    /**
     * Generate the aggregator directory path for a chosen worker
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getAggregatorWorkerPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + WORKER_SELECTED_DIR + "/" +
            getHostnamePartitionId() + AGGREGATOR_DIR;
    }

    /**
     * Generate the aggregator directory path for a superstep
     *
     * @param attempt application attempt number
     * @param superstep superstep to use
     * @return directory path based on the a superstep
     */
    final public String getAggregatorPath(long attempt, long superstep) {
        return APPLICATION_ATTEMPTS_PATH + "/" + attempt +
            SUPERSTEP_DIR + "/" + superstep + AGGREGATOR_DIR;
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
     * Get the ZooKeeperExt instance.
     *
     * @return ZooKeeperExt instance.
     */
    final public ZooKeeperExt getZkExt() {
        return m_zk;
    }

    final public Configuration getConfiguration() {
        return m_conf;
    }

    @SuppressWarnings("rawtypes")
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

    final public BspEvent getWorkerPartitionsAllReadyChangedEvent() {
        return m_workerPartitionsAllReadyChanged;
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
                      @SuppressWarnings("rawtypes") Context context,
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
        m_workerPartitionsAllReadyChanged = new ContextLock(m_context);
        m_superstepFinished = new ContextLock(m_context);
        m_superstepWorkersFinishedChanged = new ContextLock(m_context);
        m_masterElectionChildrenChanged = new ContextLock(m_context);
        m_cleanedUpChildrenChanged = new ContextLock(m_context);
        
        try {
            m_hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        m_hostnamePartitionId = m_hostname + "_" + getTaskPartition();

        BASE_PATH = BASE_DIR + "/" + m_jobId;
        MASTER_JOB_STATE_PATH = BASE_PATH + MASTER_JOB_STATE_NODE;
        INPUT_SPLIT_PATH = BASE_PATH + INPUT_SPLIT_DIR;
        INPUT_SPLITS_ALL_READY_PATH = BASE_PATH + INPUT_SPLITS_ALL_READY_NODE;
        APPLICATION_ATTEMPTS_PATH = BASE_PATH + APPLICATION_ATTEMPTS_DIR;
        CLEANED_UP_PATH = BASE_PATH + CLEANED_UP_DIR;

        LOG.info("BspService: Connecting to ZooKeeper with job " + m_jobId +
                 ", " + getTaskPartition() + " on " + serverPortList);
        try {
            m_zk = new ZooKeeperExt(serverPortList, sessionMsecTimeout, this);
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
     * Increment the superstep.
     */
     final public void incrSuperstep() {
         if (m_cachedSuperstep == -1) {
             throw new RuntimeException("incrSuperstep: Invalid -1 superstep.");
         }
         ++m_cachedSuperstep;
     }

    final public void process(WatchedEvent event) {
        LOG.info("process: Got a new event, path = " + event.getPath() +
                 ", type = " + event.getType() + ", state = " +
                 event.getState());

        /* Nothing to do unless it is a disconnect */
        if (event.getPath() == null) {
            if ((event.getType() == EventType.None) &&
                (event.getState() == KeeperState.Disconnected)) {
                m_inputSplitsAllReadyChanged.signal();
                m_inputSplitsStateChanged.signal();
                m_superstepFinished.signal();
                m_superstepWorkersFinishedChanged.signal();
                m_masterElectionChildrenChanged.signal();
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
            m_workerHealthRegistrationChanged.signal();
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
        else if (event.getPath().contains(WORKER_SELECTION_FINISHED_NODE) &&
                event.getType() == EventType.NodeCreated) {
           LOG.info("process: m_workerPartitionsAllReadyChanged signaled");
           m_workerPartitionsAllReadyChanged.signal();
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
