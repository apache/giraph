package com.yahoo.hadoop_bsp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 * @author aching
 *
 */
public class BspServiceMaster<I extends WritableComparable, V, E, M extends Writable>
    extends BspService<I, V, E, M>
    implements CentralizedServiceMaster<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
    /** Am I the master? */
    boolean m_isMaster = false;

    public BspServiceMaster(
        String serverPortList,
        int sessionMsecTimeout,
        @SuppressWarnings("rawtypes") Context context,
        BspJob.BspMapper<I, V, E, M> bspMapper) {
        super(serverPortList, sessionMsecTimeout, context, bspMapper);
    }

    /**
     * If the master decides that this job doesn't have the resources to
     * continue, it can fail the job.  Typically this is mainly informative.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void setJobState(State state) {
        if (state == BspService.State.FAILED) {
               failJob();
        }
        try {
            getZkExt().createExt(MASTER_JOB_STATE_PATH,
                                 state.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            try {
                getZkExt().setData(
                    MASTER_JOB_STATE_PATH, state.toString().getBytes(), -1);
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Master uses this to calculate the input split and write it to ZooKeeper.
     *
     * @param numSplits
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     * @throws InterruptedException
     */
    private List<InputSplit> generateInputSplits(int numSplits) {
        try {
            @SuppressWarnings({"rawtypes", "unchecked" })
            Class<VertexInputFormat> vertexInputFormatClass =
                (Class<VertexInputFormat>)
                 getConfiguration().getClass("bsp.vertexInputFormatClass",
                                 VertexInputFormat.class);
            @SuppressWarnings("unchecked")
            List<InputSplit> splits =
                vertexInputFormatClass.newInstance().getSplits(
                    getConfiguration(), numSplits);
            return splits;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * When there is no salvaging this job, fail it.
     *
     * @throws IOException
     */
    private void failJob() {
        try {
            @SuppressWarnings("deprecation")
            JobClient jobClient = new JobClient((JobConf) getConfiguration());
            @SuppressWarnings("deprecation")
            JobID jobId = JobID.forName(getJobId());
            RunningJob job = jobClient.getJob(jobId);
            LOG.fatal("failJob: Killing job " + jobId);
            job.killJob();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the healthy and unhealthy workers for a superstep
     *
     * @param superstep superstep to check
     * @param healthyWorkerList filled in with current data
     * @param unhealthyWorkerList filled in with current data
     */
    private void getWorkers(long superstep,
                            List<String> healthyWorkerList,
                            List<String> unhealthyWorkerList) {
        String healthyWorkerPath =
            getWorkerHealthyPath(getApplicationAttempt(), superstep);
        String unhealthyWorkerPath =
            getWorkerUnhealthyPath(getApplicationAttempt(), superstep);

        try {
            getZkExt().createExt(healthyWorkerPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.debug("getWorkers: " + healthyWorkerPath +
                      " already exists, no need to create.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            getZkExt().createExt(unhealthyWorkerPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("getWorkers: " + healthyWorkerPath +
                     " already exists, no need to create.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<String> currentHealthyWorkerList = null;
        List<String> currentUnhealthyWorkerList = null;
        try {
            currentHealthyWorkerList =
                getZkExt().getChildrenExt(healthyWorkerPath, true, false, false);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getWorkers: No node for " + healthyWorkerPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            currentUnhealthyWorkerList =
                getZkExt().getChildrenExt(unhealthyWorkerPath, true, false, false);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getWorkers: No node for " + unhealthyWorkerPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        healthyWorkerList.clear();
        for (String healthyWorker : currentHealthyWorkerList) {
            healthyWorkerList.add(healthyWorker);
        }

        unhealthyWorkerList.clear();
        for (String unhealthyWorker : currentUnhealthyWorkerList) {
            unhealthyWorkerList.add(unhealthyWorker);
        }
    }

    /**
     * Check the workers to ensure that a minimum number of good workers exists
     * out of the total that have reported.
     *
     * @param healthyWorkers healthy worker list
     * @param unhealthyWorkers unhealthy worker list
     * @return healthy worker list (in order)
     */
    private List<String> checkWorkers() {
        int maxPollAttempts =
            getConfiguration().getInt(BspJob.BSP_POLL_ATTEMPTS,
                                      BspJob.DEFAULT_BSP_POLL_ATTEMPTS);
        int initialProcs =
            getConfiguration().getInt(BspJob.BSP_INITIAL_PROCESSES, -1);
        int minProcs =
            getConfiguration().getInt(BspJob.BSP_MIN_PROCESSES, -1);
        float minPercentResponded =
            getConfiguration().getFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 0.0f);
        int msecsPollPeriod =
            getConfiguration().getInt(BspJob.BSP_POLL_MSECS,
                                      BspJob.DEFAULT_BSP_POLL_MSECS);
        boolean failJob = true;
        int pollAttempt = 0;
        List<String> healthyWorkerList = new ArrayList<String>();
        List<String> unhealthyWorkerList = new ArrayList<String>();
        int totalResponses = -1;
        while (pollAttempt < maxPollAttempts) {
            getWorkers(
                getSuperstep(), healthyWorkerList, unhealthyWorkerList);
            totalResponses = healthyWorkerList.size() +
                unhealthyWorkerList.size();
            if ((totalResponses * 100.0f / initialProcs) >=
                minPercentResponded) {
                failJob = false;
                break;
            }
            LOG.info("checkWorkers: Only found " + totalResponses +
                     " responses of " + initialProcs + " for superstep " +
                     getSuperstep() + ".  Sleeping for " +
                     msecsPollPeriod + " msecs and used " + pollAttempt +
                     " of " + maxPollAttempts + " attempts.");
            if (getWorkerHealthRegistrationChangedEvent().waitMsecs(
                    msecsPollPeriod)) {
                LOG.info("checkWorkers: Got event that health " +
                         "registration changed, not using poll attempt");
                getWorkerHealthRegistrationChangedEvent().reset();
                continue;
            }
            ++pollAttempt;
        }
        if (failJob) {
            LOG.warn("checkWorkers: Did not receive enough processes in " +
                     "time (only " + totalResponses + " of " +
                     minProcs + " required)");
            return null;
        }

        if (healthyWorkerList.size() < minProcs) {
            LOG.warn("checkWorkers: Only " + healthyWorkerList.size() +
                     " available when " + minProcs + " are required.");
            return null;
        }

        return healthyWorkerList;
    }

    public int createInputSplits() {
        // Only the 'master' should be doing this.  Wait until the number of
        // processes that have reported health exceeds the minimum percentage.
        // If the minimum percentage is not met, fail the job.  Otherwise
        // generate the input splits
        try {
            if (getZkExt().exists(INPUT_SPLIT_PATH, false) != null) {
                LOG.info(INPUT_SPLIT_PATH +
                         " already exists, no need to create");
                return Integer.parseInt(
                    new String(
                        getZkExt().getData(INPUT_SPLIT_PATH, false, null)));
            }
        } catch (KeeperException.NoNodeException e) {
            LOG.info("createInputSplits: Need to create the " +
                     "input splits at " + INPUT_SPLIT_PATH);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // When creating znodes, in case the master has already run, resume
        // where it left off.
        List<String> healthyWorkerList = checkWorkers();
        if (healthyWorkerList == null) {
            setJobState(State.FAILED);
        }

        List<InputSplit> splitList =
            generateInputSplits(healthyWorkerList.size());
        if (healthyWorkerList.size() > splitList.size()) {
            LOG.warn("masterGenerateInputSplits: Number of inputSplits="
                     + splitList.size() + " < " + healthyWorkerList.size() +
                     "=number of healthy processes");
        }
        else if (healthyWorkerList.size() < splitList.size()) {
            LOG.fatal("masterGenerateInputSplits: Number of inputSplits="
                      + splitList.size() + " > " + healthyWorkerList.size() +
                      "=number of healthy processes");
            setJobState(State.FAILED);
        }
        String inputSplitPath = null;
        for (int i = 0; i< splitList.size(); ++i) {
            try {
                ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream();
                DataOutput outputStream =
                    new DataOutputStream(byteArrayOutputStream);
                ((Writable) splitList.get(i)).write(outputStream);
                inputSplitPath = INPUT_SPLIT_PATH + "/" + i;
                getZkExt().createExt(inputSplitPath,
                                     byteArrayOutputStream.toByteArray(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
                LOG.info("createInputSplits: Created input split with index " +
                         i + " serialized as " +
                         byteArrayOutputStream.toString());
            } catch (KeeperException.NodeExistsException e) {
                LOG.info("createInputSplits: Node " +
                         inputSplitPath + " already exists.");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Let workers know they can start trying to load the input splits
        try {
            getZkExt().create(INPUT_SPLITS_ALL_READY_PATH,
                        null,
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("createInputSplits: Node " +
                     INPUT_SPLITS_ALL_READY_PATH + " already exists.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splitList.size();
    }

    public void setup() {
        // Nothing to do
    }

    public boolean becomeMaster() {
        /*
         * Create my bid to become the master, then try to become the worker
         * or return false.
         */
        String myBid = null;
        try {
            myBid =
                getZkExt().createExt(
                    getMasterElectionPath(getApplicationAttempt()) +
                    "/" + getHostnamePartitionId(),
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL,
                    true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        while (true) {
            if (getJobState() == State.FINISHED) {
                LOG.info("becomeMaster: Job is finished, " +
                "give up trying to be the master!");
                m_isMaster = false;
                return m_isMaster;
            }
            try {
                List<String> masterChildArr =
                    getZkExt().getChildrenExt(
                        getMasterElectionPath(getApplicationAttempt()),
                        true, true, true);
                LOG.info("becomeMaster: First child is '" +
                         masterChildArr.get(0) + "' and my bid is '" +
                         myBid + "'");
                if (masterChildArr.get(0).equals(myBid)) {
                    LOG.info("becomeMaster: I am now the master!");
                    setJobState(State.RUNNING);
                    m_isMaster = true;
                    return m_isMaster;
                }
                LOG.info("becomeMaster: Waiting to become the master...");
                getMasterElectionChildrenChangedEvent().waitForever();
                getMasterElectionChildrenChangedEvent().reset();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get the vertex range statistics for a particular superstep.
     *
     * @param superstep superstep to check
     * @return mapping of max index (vertex range) to JSONObject
     *         (# done, # total)
     */
    private Map<I, JSONObject> collectVertexRangeStats(long superstep) {
        Map<I, JSONObject> vertexRangeStatArrayMap =
            new TreeMap<I, JSONObject>();
        @SuppressWarnings({"rawtypes", "unchecked" })
        Class<? extends WritableComparable> indexClass =
            (Class<? extends WritableComparable>)
            getConfiguration().getClass("bsp.indexClass",
                                        WritableComparable.class);
        // Superstep 0 is special since there is no computation, just get
        // the stats from the input splits finished nodes.  Otherwise, get the
        // stats from the all the worker selected nodes
        if (superstep == 0) {
            List<String> inputSplitList = null;
            try {
                inputSplitList = getZkExt().getChildrenExt(
                    INPUT_SPLIT_PATH, false, false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            for (String inputSplitPath : inputSplitList) {
                JSONArray statArray = null;
                try {
                    String inputSplitFinishedPath = inputSplitPath +
                        INPUT_SPLIT_FINISHED_NODE;
                    byte [] zkData =
                        getZkExt().getData(inputSplitFinishedPath, false, null);
                    if (zkData == null || zkData.length == 0) {
                        continue;
                    }
                    LOG.info("collectVertexRangeStats: input split path " +
                             inputSplitPath + " got " + zkData);
                    statArray = new JSONArray(new String(zkData));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < statArray.length(); ++i) {
                    try {
                        @SuppressWarnings("unchecked")
                        I maxIndex = (I) indexClass.newInstance();
                        LOG.info("collectVertexRangeStats: " +
                                 "Getting max index from " +
                                 statArray.getJSONObject(i).get(
                                     STAT_MAX_INDEX_KEY));
                        InputStream input =
                            new ByteArrayInputStream(
                                statArray.getJSONObject(i).getString(
                                    STAT_MAX_INDEX_KEY).getBytes("UTF-8"));
                        ((Writable) maxIndex).readFields(
                            new DataInputStream(input));
                        statArray.getJSONObject(i).put(
                            STAT_FINISHED_VERTICES_KEY,
                            0);
                        vertexRangeStatArrayMap.put(maxIndex,
                                                    statArray.getJSONObject(i));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        else {
            String workerSelectedPath =
                getWorkerSelectedPath(getApplicationAttempt(), superstep);
            List<String> hostnameIdPathList = null;
            try {
                hostnameIdPathList =
                    getZkExt().getChildrenExt(
                        workerSelectedPath, false, false, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            for (String hostnameIdPath : hostnameIdPathList) {
                String vertexRangeStatsPath =
                    hostnameIdPath + VERTEX_RANGE_STATS_DIR;
                JSONArray statArray = null;
                try {
                    byte [] zkData =
                        getZkExt().getData(vertexRangeStatsPath, false, null);
                    statArray = new JSONArray(new String(zkData));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < statArray.length(); ++i) {
                    try {
                        @SuppressWarnings("unchecked")
                        I maxIndex = (I) indexClass.newInstance();
                        LOG.info("collectVertexRangeStats: " +
                                 "Getting max index from " +
                                 statArray.getJSONObject(i).get(
                                     STAT_MAX_INDEX_KEY));
                        InputStream input =
                            new ByteArrayInputStream(
                                statArray.getJSONObject(i).getString(
                                    STAT_MAX_INDEX_KEY).getBytes("UTF-8"));
                        ((Writable) maxIndex).readFields(
                            new DataInputStream(input));
                        vertexRangeStatArrayMap.put(maxIndex,
                                                    statArray.getJSONObject(i));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
         }

        return vertexRangeStatArrayMap;
    }

    /**
     * Assign vertex ranges to the workers.
     *
     * @param chosenWorkerList selected workers that are healthy
     */
    private void assignWorkersVertexRanges(List<String> chosenWorkerList) {
        // TODO: Actually pass vertex ranges around
        //
        // Current algorithm: Take the assign the original owners of every
        // partition to that partition.  If the original owner no longer
        // exists, fail!
        List<String> inputSplitList = null;
        try {
            inputSplitList = getZkExt().getChildrenExt(INPUT_SPLIT_PATH,
                                                       false,
                                                       false,
                                                       true);
            for (String inputSplitPath : inputSplitList) {
                String inputSplitFinishedPath = inputSplitPath +
                    INPUT_SPLIT_FINISHED_NODE;
                byte [] zkData =
                    getZkExt().getData(inputSplitFinishedPath, false, null);
                if (zkData == null || zkData.length == 0) {
                    LOG.info("assignWorkersVertexRanges: No vertex ranges " +
                             "in " + inputSplitFinishedPath);
                    continue;
                }
                JSONArray statArray = new JSONArray(new String(zkData));
                JSONArray vertexRangeArray = new JSONArray();
                JSONObject stat = null;
                for (int i = 0; i < statArray.length(); ++i) {
                    stat = statArray.getJSONObject(i);
                    vertexRangeArray.put(stat.get(STAT_MAX_INDEX_KEY));
                }
                String hostnamePartitionId =
                    stat.getString(STAT_HOSTNAME_ID_KEY);
                if (!chosenWorkerList.contains(hostnamePartitionId)) {
                    LOG.fatal("assignWorkersVertexRanges: Worker " +
                              hostnamePartitionId + " died, failing.");
                    setJobState(State.FAILED);
                }
                String workerSelectedPath =
                        getWorkerSelectedPath(getApplicationAttempt(),
                                              getSuperstep()) +
                                              "/" + hostnamePartitionId;
                LOG.info("assignWorkersVertexRanges: Assigning " +
                         workerSelectedPath + " = " +
                         vertexRangeArray.toString());
                // Worker may have possibly already processed one input split
                if (getZkExt().exists(workerSelectedPath, null) != null) {
                    Stat initialStat = new Stat();
                    byte [] initialZkData =
                        getZkExt().getData(
                            workerSelectedPath, false, initialStat);
                    JSONArray initialVertexRangeArray =
                        new JSONArray(new String(initialZkData));
                    for (int i = 0; i < vertexRangeArray.length(); ++i) {
                        initialVertexRangeArray.put(vertexRangeArray.get(i));
                    }
                    getZkExt().setData(workerSelectedPath,
                                       initialVertexRangeArray.toString().getBytes(),
                                       initialStat.getVersion());
                }
                else {
                    getZkExt().createExt(workerSelectedPath,
                                         vertexRangeArray.toString().getBytes(),
                                         Ids.OPEN_ACL_UNSAFE,
                                         CreateMode.PERSISTENT,
                                         true);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean coordinateSuperstep() {
        // 1. Get chosen workers.
        // 2. Assign partitions to the workers
        // 3. Wait for all workers to complete
        // 4. Create superstep finished node

        List<String> chosenWorkerList = checkWorkers();
        if (chosenWorkerList == null) {
            LOG.fatal("coordinateSuperstep: Not enough healthy workers for " +
                      "superstep " + getSuperstep());
            setJobState(State.FAILED);
        }

        if (getSuperstep() != 0) {
            assignWorkersVertexRanges(chosenWorkerList);
        }

        String workerSelectionFinishedPath =
            getWorkerSelectionFinishedPath(getApplicationAttempt(),
                                           getSuperstep());
        try {
            getZkExt().createExt(workerSelectionFinishedPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("coordinateSuperstep: Node " +
                     workerSelectionFinishedPath + " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String finishedWorkerPath =
            getWorkerFinishedPath(getApplicationAttempt(), getSuperstep());
        try {
            getZkExt().createExt(finishedWorkerPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("coordinateBarrier: finishedWorkers " +
                     finishedWorkerPath + " already exists, no need to create");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        while (true) {
            List<String> finishedWorkerList = null;
            try {
                finishedWorkerList =
                    getZkExt().getChildrenExt(finishedWorkerPath,
                                              true,
                                              false,
                                              false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("coordinateSuperstep: Got finished worker list = " +
                     finishedWorkerList + ", size = " +
                     finishedWorkerList.size() + ", chosen worker list = " +
                     chosenWorkerList + ", size = " + chosenWorkerList.size() +
                     " from " + finishedWorkerPath);
            if (finishedWorkerList.containsAll(chosenWorkerList)) {
                break;
            }
            getSuperstepWorkersFinishedChangedEvent().waitForever();
            getSuperstepWorkersFinishedChangedEvent().reset();
        }
        Map<I, JSONObject> maxIndexStatsMap =
            collectVertexRangeStats(getSuperstep());
        long verticesFinished = 0;
        long verticesTotal = 0;
        for (Map.Entry<I, JSONObject> entry : maxIndexStatsMap.entrySet()) {
            try {
                verticesFinished +=
                    entry.getValue().getLong(STAT_FINISHED_VERTICES_KEY);
                verticesTotal +=
                    entry.getValue().getLong(STAT_NUM_VERTICES_KEY);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("coordinateSuperstep: Aggregate got " + verticesFinished +
                 " of " + verticesTotal + " vertices finished on superstep = " +
                 getSuperstep());
        String superstepFinishedNode =
            getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
        // Let everyone know the aggregated application state through the
        // superstep
        try {
            JSONObject globalInfoObject = new JSONObject();
            globalInfoObject.put(STAT_FINISHED_VERTICES_KEY, verticesFinished);
            globalInfoObject.put(STAT_NUM_VERTICES_KEY, verticesTotal);
            getZkExt().createExt(superstepFinishedNode,
                                 globalInfoObject.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Clean up the old supersteps
        if ((getSuperstep() - 1) >= 0) {
            String oldSuperstepPath =
                getSuperstepPath(getApplicationAttempt()) + "/" +
                (getSuperstep() - 1);
            try {
                LOG.info("coordinateSuperstep: Cleaning up old Superstep " +
                         oldSuperstepPath);
                getZkExt().deleteExt(oldSuperstepPath,
                                     -1,
                                     true);
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("coordinateBarrier: Already cleaned up " +
                         oldSuperstepPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        incrSuperstep();
        return (verticesFinished == verticesTotal);
    }

    private void cleanUpZooKeeper() {
        try {
            getZkExt().createExt(CLEANED_UP_PATH,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("cleanUpZooKeeper: Node " + CLEANED_UP_PATH +
                      " already exists, no need to create.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Note that need to wait for 2 * the partitions (worker and master)
        List<String> cleanedUpChildrenList = null;
        while (true) {
            try {
                cleanedUpChildrenList =
                    getZkExt().getChildrenExt(CLEANED_UP_PATH, true, false, true);
                LOG.info("cleanUpZooKeeper: Got " +
                         cleanedUpChildrenList.size() + " of " +
                         2 * getTotalMappers() +  " desired children from " +
                         CLEANED_UP_PATH);
                if (cleanedUpChildrenList.size() == 2 * getTotalMappers()) {
                    break;
                }
                LOG.info("cleanedUpZooKeeper: Waiting for the children of " +
                         CLEANED_UP_PATH + " to change since only got " +
                         cleanedUpChildrenList.size() + " nodes.");
            }
            catch (Exception e) {
                // We are in the cleanup phase -- just log the error
                LOG.error(e.getMessage());
                return;
            }

            getCleanedUpChildrenChangedEvent().waitForever();
            getCleanedUpChildrenChangedEvent().reset();
        }

        /*
         * At this point, all processes have acknowledged the cleanup,
         * and the master can do any final cleanup
         */
        try {
            LOG.info("cleanupZooKeeper: Removing the following path and all " +
                     "children - " + BASE_PATH);
            getZkExt().deleteExt(BASE_PATH, -1, true);
        } catch (Exception e) {
            LOG.error("cleanupZooKeeper: Failed to do cleanup of " + BASE_PATH);
        }
    }

    public void cleanup() {
        // All master processes should denote they are done by adding special
        // znode.  Once the number of znodes equals the number of partitions
        // for workers and masters, the master will clean up the ZooKeeper
        // znodes associated with this job.
       String cleanedUpPath = CLEANED_UP_PATH  + "/" +
           getTaskPartition() + MASTER_SUFFIX;
       try {
           String finalFinishedPath =
               getZkExt().createExt(cleanedUpPath,
                                    null,
                                    Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT,
                                    true);
            LOG.info("cleanup: Notifying master its okay to cleanup with " +
                    finalFinishedPath);
       } catch (KeeperException.NodeExistsException e) {
           LOG.info("cleanup: Couldn't create finished node '" +
                    cleanedUpPath);
       } catch (Exception e) {
           // cleanup phase -- just log the error
           LOG.error(e.getMessage());
       }

       if (m_isMaster) {
           cleanUpZooKeeper();
       }

       try {
           getZkExt().close();
       } catch (InterruptedException e) {
           // cleanup phase -- just log the error
           LOG.error("cleanup: Zookeeper failed to close with " + e);
       }
   }
}
