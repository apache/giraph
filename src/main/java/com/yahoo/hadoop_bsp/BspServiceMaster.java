package com.yahoo.hadoop_bsp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.codec.binary.Base64;

import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
@SuppressWarnings({ "deprecation", "rawtypes" })
public class BspServiceMaster<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable>
    extends BspService<I, V, E, M>
    implements CentralizedServiceMaster<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
    /** Am I the master? */
    boolean m_isMaster = false;


    public BspServiceMaster(
        String serverPortList,
        int sessionMsecTimeout,
        Context context,
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
            LOG.info("setJobState: " + state);
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
            @SuppressWarnings({"unchecked" })
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
        LOG.fatal("failJob: Killing job " + getJobId());
        try {
            JobClient jobClient = new JobClient((JobConf) getConfiguration());
            JobID jobId = JobID.forName(getJobId());
            RunningJob job = jobClient.getJob(jobId);
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

    /**
     * Read the metadata files for the checkpoint, determine the worker
     * assignments for vertex ranges, and assign the metadata files.  Remove
     * any unused chosen workers (this is safe since it wasn't finalized).
     * The workers can then find the vertex ranges within the files.  It is an
     * optimization to prevent all workers from searching all the files.
     * Also read in the aggregator data from the finalized checkpoint
     * file.
     *
     * @param superstep checkpoint set to examine.
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void mapFilesToWorkers(
        long superstep,
        List<String> chosenWorkerList)
        throws IOException, KeeperException, InterruptedException {
        FileSystem fs = getFs();
        List<Path> validMetadataPathList = new ArrayList<Path>();
        String finalizedCheckpointPath =
            getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
        DataInputStream finalizedStream =
            fs.open(new Path(finalizedCheckpointPath));
        int prefixCount = finalizedStream.readInt();
        for (int i = 0; i < prefixCount; ++i) {
            String metadataFilePath =
                finalizedStream.readUTF() + CHECKPOINT_METADATA_POSTFIX;
            validMetadataPathList.add(new Path(metadataFilePath));
        }

        // Set the aggregator data if it exists.
        int aggregatorDataSize = finalizedStream.readInt();
        if (aggregatorDataSize > 0) {
            byte [] aggregatorZkData = new byte[aggregatorDataSize];
            int actualDataRead =
                finalizedStream.read(aggregatorZkData, 0, aggregatorDataSize);
            if (actualDataRead != aggregatorDataSize) {
                throw new RuntimeException(
                    "mapFilesToWorkers: Only read " + actualDataRead + " of " +
                    aggregatorDataSize + " aggregator bytes from " +
                    finalizedCheckpointPath);
            }
            String aggregatorPath =
                getAggregatorPath(getApplicationAttempt(), superstep - 1);
            LOG.info("mapFilesToWorkers: Reloading aggregator data '" +
                     aggregatorZkData + "' to previous checkopint " +
                     aggregatorPath);
            if (getZkExt().exists(aggregatorPath, false) == null) {
                getZkExt().createExt(aggregatorPath,
                                     aggregatorZkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            }
            else {
                getZkExt().setData(aggregatorPath, aggregatorZkData, -1);
            }
        }

        finalizedStream.close();

        I maxIndex;
        try {
            maxIndex = createVertexIndex();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Map<String, Set<String>> workerFileSetMap =
            new HashMap<String, Set<String>>();
        // Reading the metadata files.  For now, implement simple algorithm:
        // 1. Every file is set an input split and the partitions are mapped
        //    accordingly
        // 2. Every worker gets a hint about which files to look in to find
        //    the input splits
        int chosenWorkerListIndex = 0;
        int inputSplitIndex = 0;
        for (Path metadataPath : validMetadataPathList) {
            DataInputStream metadataStream = fs.open(metadataPath);
            long entries = metadataStream.readLong();
            JSONArray vertexRangeMetaArray = new JSONArray();
            JSONArray vertexRangeArray = new JSONArray();
            String chosenWorker =
                chosenWorkerList.get(chosenWorkerListIndex);
            for (long i = 0; i < entries; ++i) {
                long dataPos = metadataStream.readLong();
                Long vertexCount = metadataStream.readLong();
                maxIndex.readFields(metadataStream);
                LOG.debug("mapFileToWorkers: File " + metadataPath +
                          " with position " + dataPos + ", vertex count " +
                          vertexCount + " assigned to " + chosenWorker);
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                maxIndex.write(output);

                JSONObject vertexRangeObj = new JSONObject();
                try {
                    vertexRangeObj.put(STAT_NUM_VERTICES_KEY,
                                       vertexCount);
                    vertexRangeObj.put(STAT_HOSTNAME_ID_KEY,
                                       chosenWorker);
                    vertexRangeObj.put(STAT_MAX_INDEX_KEY,
                                       outputStream.toString("UTF-8"));
                    vertexRangeMetaArray.put(vertexRangeObj);
                    vertexRangeArray.put(outputStream.toString("UTF-8"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (!workerFileSetMap.containsKey(chosenWorker)) {
                    workerFileSetMap.put(chosenWorker, new HashSet<String>());
                }
                Set<String> fileSet = workerFileSetMap.get(chosenWorker);
                fileSet.add(metadataPath.toString());
            }
            metadataStream.close();

            String inputSplitPathFinishedPath =
                INPUT_SPLIT_PATH + "/" + inputSplitIndex +
                INPUT_SPLIT_FINISHED_NODE;
            String workerSelectedPath =
                getWorkerSelectedPath(getApplicationAttempt(), superstep) +
                "/" + chosenWorker;
            LOG.info("mapFilesToWorkers: Assigning (if not empty)" +
                     workerSelectedPath + " the following vertexRanges: " +
                     vertexRangeArray.toString());

            // Assign the input splits and the vertex ranges if there
            // is at least one vertex range
            if (vertexRangeArray.length() == 0) {
                continue;
            }
            try {
                 getZkExt().createExt(inputSplitPathFinishedPath,
                                     vertexRangeMetaArray.toString().getBytes(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
                getZkExt().createExt(workerSelectedPath,
                                     vertexRangeArray.toString().getBytes(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ++inputSplitIndex;
            ++chosenWorkerListIndex;
            if (chosenWorkerListIndex == chosenWorkerList.size()) {
                chosenWorkerListIndex = 0;
            }
        }

        // Dump the file mapping information into ZooKeeper
        String workerPath = null;
        for (Map.Entry<String, Set<String>> entry :
             workerFileSetMap.entrySet()) {
            workerPath = getCheckpointFilesPath(getApplicationAttempt(),
                                                getSuperstep(),
                                                entry.getKey());
            JSONArray fileArray = new JSONArray(entry.getValue());
            try {
                getZkExt().createExt(workerPath,
                                     fileArray.toString().getBytes(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("mapFileToWorkers: Already existing - " + workerPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setup() {
        // Might have to manually load a checkpoint.
        // In that case, the input splits are not set, they will be faked by
        // the checkpoint files.  Each checkpoint file will be an input split
        // and the input split
        if (getManualRestartSuperstep() == -1) {
            return;
        }
        else if (getManualRestartSuperstep() < -1) {
            LOG.fatal("setup: Impossible to restart superstep " +
                      getManualRestartSuperstep());
            setJobState(BspService.State.FAILED);
        }
    }

    public boolean becomeMaster() {
        // Create my bid to become the master, then try to become the worker
        // or return false.
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
                        I maxIndex = createVertexIndex();
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
                        I maxIndex = createVertexIndex();
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
     * Get the aggregator values for a particular superstep,
     * aggregate and save them.
     *
     * @param superstep superstep to check
     */
    private void collectAndProcessAggregatorValues(long superstep) {
        if (superstep <= 1) {
            return;
        }
        Map<String, Aggregator<Writable>> aggregatorMap =
            new TreeMap<String, Aggregator<Writable>>();
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

        Base64 base64 = new Base64();
        for (String hostnameIdPath : hostnameIdPathList) {
            String aggregatorPath =
                hostnameIdPath + AGGREGATOR_DIR;
            JSONArray aggregatorArray = null;
            try {
                byte [] zkData =
                    getZkExt().getData(aggregatorPath, false, null);
                aggregatorArray = new JSONArray(new String(zkData));
            } catch (KeeperException.NoNodeException e) {
                LOG.info("collectAndProcessAggregatorValues: " + 
                         "no aggregators in " + aggregatorPath);
            } catch (Exception e) {
               throw new RuntimeException(
                         "collectAndProcessAggregatorValues: " +
                         "exception when fetching data from " +
                         aggregatorPath, e);
            }
            if (aggregatorArray == null) {
                continue;
            }
            for (int i = 0; i < aggregatorArray.length(); ++i) {
                try {
                    LOG.info("collectAndProcessAggregatorValues: " +
                             "Getting aggregators from " +
                              aggregatorArray.getJSONObject(i));
                    String aggregatorName = aggregatorArray.getJSONObject(i).
                        getString(AGGREGATOR_NAME_KEY);
                    Aggregator<Writable> aggregator = aggregatorMap.get(
                                                               aggregatorName);
                    boolean firstTime = false;
                    if (aggregator == null) {
                        aggregator =
                            BspJob.BspMapper.getAggregator(aggregatorName);
                        aggregatorMap.put(aggregatorName, aggregator);
                        firstTime = true;
                    }
                    Writable aggregatorValue =
                                            aggregator.createAggregatedValue();
                    InputStream input =
                        new ByteArrayInputStream(
                            (byte[]) base64.decode(
                                aggregatorArray.getJSONObject(i).
                                getString(AGGREGATOR_VALUE_KEY)));
                    aggregatorValue.readFields(new DataInputStream(input));
                    LOG.debug("collectAndProcessAggregatorValues: aggregator " +
                              "value size=" + input.available() +
                              " for aggregator=" + aggregatorName +
                              " value=" + aggregatorValue);
                    if (firstTime) {
                        aggregator.setAggregatedValue(aggregatorValue);
                    } else {
                        aggregator.aggregate(aggregatorValue);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                             "collectAndProcessAggregatorValues: " +
                             "exception when reading aggregator data " +
                             aggregatorArray, e);
                }
            }
        }
        if (aggregatorMap.size() > 0) {
            String aggregatorPath =
                getAggregatorPath(getApplicationAttempt(), superstep);
            byte [] zkData = null;
            JSONArray aggregatorArray = new JSONArray();
            for (Map.Entry<String, Aggregator<Writable>> entry :
                    aggregatorMap.entrySet()) {
                try {
                    ByteArrayOutputStream outputStream =
                        new ByteArrayOutputStream();
                    DataOutput output = new DataOutputStream(outputStream);
                    entry.getValue().getAggregatedValue().write(output);

                    JSONObject aggregatorObj = new JSONObject();
                    aggregatorObj.put(AGGREGATOR_NAME_KEY,
                                      entry.getKey());
                    aggregatorObj.put(
                        AGGREGATOR_VALUE_KEY,
                        base64.encodeToString(outputStream.toByteArray()));
                    aggregatorArray.put(aggregatorObj);
                    LOG.info("collectAndProcessAggregatorValues: " +
                             "Trying to add aggregatorObj " +
                             aggregatorObj + "(" +
                             entry.getValue().getAggregatedValue() +
                             ") to aggregator path " + aggregatorPath);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                zkData = aggregatorArray.toString().getBytes();
                getZkExt().createExt(aggregatorPath,
                                     zkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("collectAndProcessAggregatorValues: " +
                         aggregatorPath +
                         " already exists!");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("collectAndProcessAggregatorValues: Finished loading " +
                     aggregatorPath + " with aggregator values " +
                     aggregatorArray);
        }
    }

    /**
     * Assign vertex ranges to the workers.  Also filled in
     * m_vertexRangeMaxWorkerMap with the assignment of vertex ranges to
     * workers.
     *
     * @param chosenWorkerList selected workers that are healthy
     */
    private void assignWorkersVertexRanges(List<String> chosenWorkerList) {
        // TODO: Actually pass vertex ranges around
        //
        // Current algorithm: Assign the original owners of every
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

    /**
     * Finalize the checkpoint file prefixes by taking the chosen workers and
     * writing them to a finalized file.  Also write out the master
     * aggregated aggregator array from the previous superstep.
     *
     * @param superstep superstep to finalize
     * @param chosenWorkerList list of chosen workers that will be finalized
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void finalizeCheckpoint(
        long superstep,
        List<String> chosenWorkerList)
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
        // <number of files>
        // <used file prefix 0><used file prefix 1>...
        // <aggregator data length><aggregators as a serialized JSON byte array>
        FSDataOutputStream finalizedOutputStream =
            getFs().create(finalizedCheckpointPath);
        finalizedOutputStream.writeInt(chosenWorkerList.size());
        for (String chosenWorker : chosenWorkerList) {
            String chosenWorkerPrefix =
                getCheckpointBasePath(superstep) + "." + chosenWorker;
            finalizedOutputStream.writeUTF(chosenWorkerPrefix);
        }
        String aggregatorPath =
            getAggregatorPath(getApplicationAttempt(), superstep - 1);
        if (getZkExt().exists(aggregatorPath, false) != null) {
            byte [] aggregatorZkData =
                getZkExt().getData(aggregatorPath, false, null);
            finalizedOutputStream.writeInt(aggregatorZkData.length);
            finalizedOutputStream.write(aggregatorZkData);
        }
        else {
            finalizedOutputStream.writeInt(0);
        }
        finalizedOutputStream.close();
    }

    public boolean coordinateSuperstep() {
        // 1. Get chosen workers.
        // 2. Assign partitions to the workers
        //    or possibly reload from a superstep
        // 3. Wait for all workers to complete
        // 4. Collect and process aggregators
        // 5. Create superstep finished node
        // 6. If the checkpoint frequency is met, finalize the checkpoint

        List<String> chosenWorkerList = checkWorkers();
        if (chosenWorkerList == null) {
            LOG.fatal("coordinateSuperstep: Not enough healthy workers for " +
                      "superstep " + getSuperstep());
            setJobState(State.FAILED);
        }

        if (getManualRestartSuperstep() == getSuperstep()) {
            try {
                LOG.info("coordinateSuperstep: Reloading from superstep " +
                         getSuperstep());

                mapFilesToWorkers(getManualRestartSuperstep(), chosenWorkerList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            if (getSuperstep() != 0) {
                assignWorkersVertexRanges(chosenWorkerList);
            }
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
        collectAndProcessAggregatorValues(getSuperstep());
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

        // Finalize the valid checkpoint file prefixes and possibly
        // the aggregators.
        if (checkpointFrequencyMet(getSuperstep())) {
            try {
                finalizeCheckpoint(getSuperstep(), chosenWorkerList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Clean up the old supersteps
        if ((getConfiguration().getBoolean(
                BspJob.BSP_KEEP_ZOOKEEPER_DATA,
                BspJob.DEFAULT_BSP_KEEP_ZOOKEEPER_DATA) == false) &&
            ((getSuperstep() - 1) >= 0)) {
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

        incrCachedSuperstep();
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

         // At this point, all processes have acknowledged the cleanup,
         // and the master can do any final cleanup
        try {
            if (getConfiguration().getBoolean(
                BspJob.BSP_KEEP_ZOOKEEPER_DATA,
                BspJob.DEFAULT_BSP_KEEP_ZOOKEEPER_DATA) == false) {
                LOG.info("cleanupZooKeeper: Removing the following path " +
                         "and all children - " + BASE_PATH);
                getZkExt().deleteExt(BASE_PATH, -1, true);
            }
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
