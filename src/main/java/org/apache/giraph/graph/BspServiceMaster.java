/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.codec.binary.Base64;

import org.apache.log4j.Logger;
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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.CentralizedService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.graph.GiraphJob.BspMapper.MapFunctions;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;


/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 */
@SuppressWarnings("rawtypes")
public class BspServiceMaster<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable, M extends Writable>
        extends BspService<I, V, E, M>
        implements CentralizedServiceMaster<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
    /** Synchronizes vertex ranges */
    private Object vertexRangeSynchronization = new Object();
    /** Superstep counter */
    private Counter superstepCounter = null;
    /** Vertex counter */
    private Counter vertexCounter = null;
    /** Finished vertex counter */
    private Counter finishedVertexCounter = null;
    /** Edge counter */
    private Counter edgeCounter = null;
    /** Workers on this superstep */
    private Counter currentWorkersCounter = null;
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
    /** State of the superstep changed */
    private final BspEvent superstepStateChanged =
        new PredicateLock();

    public BspServiceMaster(
            String serverPortList,
            int sessionMsecTimeout,
            Mapper<?, ?, ?, ?>.Context context,
            GiraphJob.BspMapper<I, V, E, M> bspMapper) {
        super(serverPortList, sessionMsecTimeout, context, bspMapper);
        registerBspEvent(superstepStateChanged);

        maxWorkers =
            getConfiguration().getInt(GiraphJob.MAX_WORKERS, -1);
        minWorkers =
            getConfiguration().getInt(GiraphJob.MIN_WORKERS, -1);
        minPercentResponded =
            getConfiguration().getFloat(GiraphJob.MIN_PERCENT_RESPONDED,
                                        100.0f);
        msecsPollPeriod =
            getConfiguration().getInt(GiraphJob.POLL_MSECS,
                                      GiraphJob.POLL_MSECS_DEFAULT);
        maxPollAttempts =
            getConfiguration().getInt(GiraphJob.POLL_ATTEMPTS,
                                      GiraphJob.POLL_ATTEMPTS_DEFAULT);
        partitionLongTailMinPrint = getConfiguration().getInt(
            GiraphJob.PARTITION_LONG_TAIL_MIN_PRINT,
            GiraphJob.PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT);
    }

    @Override
    public void setJobState(State state,
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
            getZkExt().createExt(MASTER_JOB_STATE_PATH + "/jobState",
                                 jobState.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT_SEQUENTIAL,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            throw new IllegalStateException(
                "setJobState: Imposible that " +
                MASTER_JOB_STATE_PATH + " already exists!", e);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "setJobState: Unknown KeeperException for " +
                MASTER_JOB_STATE_PATH, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "setJobState: Unknown InterruptedException for " +
                MASTER_JOB_STATE_PATH, e);
        }

        if (state == BspService.State.FAILED) {
            failJob();
        }
    }

    /**
     * Master uses this to calculate the {@link VertexInputFormat}
     * input splits and write it to ZooKeeper.
     *
     * @param numWorkers Number of available workers
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     * @throws InterruptedException
     */
    private List<InputSplit> generateInputSplits(int numWorkers) {
        VertexInputFormat<I, V, E> vertexInputFormat =
            BspUtils.<I, V, E>createVertexInputFormat(getConfiguration());
        List<InputSplit> splits;
        try {
            splits = vertexInputFormat.getSplits(getContext(), numWorkers);
            return splits;
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
                    getConfiguration());
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("getWorkers: " + healthyWorkerPath +
                          " already exists, no need to create.");
            }
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        try {
            getZkExt().createExt(unhealthyWorkerPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getWorkers: " + healthyWorkerPath +
                          " already exists, no need to create.");
            }
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        List<String> currentHealthyWorkerList = null;
        List<String> currentUnhealthyWorkerList = null;
        try {
            currentHealthyWorkerList =
                getZkExt().getChildrenExt(healthyWorkerPath, true, false, false);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getWorkers: No node for " + healthyWorkerPath);
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        try {
            currentUnhealthyWorkerList =
                getZkExt().getChildrenExt(unhealthyWorkerPath, true, false, false);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getWorkers: No node for " + unhealthyWorkerPath);
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        healthyWorkerList.clear();
        if (currentHealthyWorkerList != null) {
            for (String healthyWorker : currentHealthyWorkerList) {
                healthyWorkerList.add(healthyWorker);
            }
        }

        unhealthyWorkerList.clear();
        if (currentUnhealthyWorkerList != null) {
            for (String unhealthyWorker : currentUnhealthyWorkerList) {
                unhealthyWorkerList.add(unhealthyWorker);
            }
        }
    }

    /**
     * Check the workers to ensure that a minimum number of good workers exists
     * out of the total that have reported.
     *
     * @return map of healthy worker list to JSONArray(hostname, port)
     */
    private Map<String, JSONArray> checkWorkers() {
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
            if ((totalResponses * 100.0f / maxWorkers) >=
                    minPercentResponded) {
                failJob = false;
                break;
            }
            getContext().setStatus(getBspMapper().getMapFunctions() + " " +
                                   "checkWorkers: Only found " + totalResponses +
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
                if ((maxWorkers - totalResponses) <= partitionLongTailMinPrint) {
                    Set<Integer> partitionSet = new TreeSet<Integer>();
                    for (String hostnamePartitionId : healthyWorkerList) {
                        int lastIndex = hostnamePartitionId.lastIndexOf("_");
                        Integer partition = Integer.parseInt(
                            hostnamePartitionId.substring(lastIndex + 1));
                        partitionSet.add(partition);
                    }
                    for (String hostnamePartitionId : unhealthyWorkerList) {
                        int lastIndex = hostnamePartitionId.lastIndexOf("_");
                        Integer partition = Integer.parseInt(
                            hostnamePartitionId.substring(lastIndex + 1));
                        partitionSet.add(partition);
                    }
                    for (int i = 1; i <= maxWorkers; ++i) {
                        if (partitionSet.contains(new Integer(i))) {
                            continue;
                        } else if (i == getTaskPartition()) {
                            continue;
                        } else {
                            LOG.info("checkWorkers: No response from "+
                                     "partition " + i + " (could be master)");
                        }
                    }
                }
            }
            ++pollAttempt;
        }
        if (failJob) {
            LOG.warn("checkWorkers: Did not receive enough processes in " +
                     "time (only " + totalResponses + " of " +
                     minWorkers + " required)");
            return null;
        }

        if (healthyWorkerList.size() < minWorkers) {
            LOG.warn("checkWorkers: Only " + healthyWorkerList.size() +
                     " available when " + minWorkers + " are required.");
            return null;
        }

        Map<String, JSONArray> workerHostnamePortMap =
            new HashMap<String, JSONArray>();
        for (String healthyWorker: healthyWorkerList) {
            String healthyWorkerPath = null;
            try {
                healthyWorkerPath =
                    getWorkerHealthyPath(getApplicationAttempt(),
                                         getSuperstep()) + "/" +  healthyWorker;
                JSONArray hostnamePortArray =
                    new JSONArray(
                        new String(getZkExt().getData(healthyWorkerPath,
                                                      false,
                                                      null)));
                workerHostnamePortMap.put(healthyWorker, hostnamePortArray);
            } catch (JSONException e) {
                throw new RuntimeException(
                    "checkWorkers: Problem fetching hostname and port for " +
                    healthyWorker + " in " + healthyWorkerPath);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "checkWorkers: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "checkWorkers: IllegalStateException", e);
            }
        }

        return workerHostnamePortMap;
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
            if (LOG.isInfoEnabled()) {
                LOG.info("createInputSplits: Need to create the " +
                         "input splits at " + INPUT_SPLIT_PATH);
            }
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "createInputSplits: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "createInputSplits: IllegalStateException", e);
        }

        // When creating znodes, in case the master has already run, resume
        // where it left off.
        Map<String, JSONArray> healthyWorkerHostnamePortMap = checkWorkers();
        if (healthyWorkerHostnamePortMap == null) {
            setJobState(State.FAILED, -1, -1);
        }

        List<InputSplit> splitList =
            generateInputSplits(healthyWorkerHostnamePortMap.size());
        if (healthyWorkerHostnamePortMap.size() > splitList.size()) {
            LOG.warn("createInputSplits: Number of inputSplits="
                     + splitList.size() + " < " +
                     healthyWorkerHostnamePortMap.size() +
                     "=number of healthy processes");
        }
        else if (healthyWorkerHostnamePortMap.size() < splitList.size()) {
            LOG.fatal("masterGenerateInputSplits: Number of inputSplits="
                      + splitList.size() + " > " +
                      healthyWorkerHostnamePortMap.size() +
                      "=number of healthy processes");
            setJobState(State.FAILED, -1, -1);
        }
        String inputSplitPath = null;
        for (int i = 0; i< splitList.size(); ++i) {
            try {
                ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream();
                DataOutput outputStream =
                    new DataOutputStream(byteArrayOutputStream);
                InputSplit inputSplit = splitList.get(i);
                Text.writeString(outputStream,
                                 inputSplit.getClass().getName());
                ((Writable) inputSplit).write(outputStream);
                inputSplitPath = INPUT_SPLIT_PATH + "/" + i;
                getZkExt().createExt(inputSplitPath,
                                     byteArrayOutputStream.toByteArray(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("createInputSplits: Created input split " +
                              "with index " + i + " serialized as " +
                              byteArrayOutputStream.toString());
                }
            } catch (KeeperException.NodeExistsException e) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("createInputSplits: Node " +
                             inputSplitPath + " already exists.");
                }
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "createInputSplits: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "createInputSplits: IllegalStateException", e);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "createInputSplits: IOException", e);
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
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "createInputSplits: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "createInputSplits: IllegalStateException", e);
        }

        return splitList.size();
    }

    /**
     * Read the finalized checkpoint file and associated metadata files for the
     * checkpoint.  Assign one worker per checkpoint point (and associated
     * vertex ranges).  Remove any unused chosen workers
     * (this is safe since it wasn't finalized).  The workers can then
     * find the vertex ranges within the files.  It is an
     * optimization to prevent all workers from searching all the files.
     * Also read in the aggregator data from the finalized checkpoint
     * file.
     *
     * @param superstep checkpoint set to examine.
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void mapFilesToWorkers(long superstep,
                                   List<String> chosenWorkerList)
            throws IOException, KeeperException, InterruptedException {
        FileSystem fs = getFs();
        List<Path> validMetadataPathList = new ArrayList<Path>();
        String finalizedCheckpointPath =
            getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
        DataInputStream finalizedStream =
            fs.open(new Path(finalizedCheckpointPath));
        int prefixFileCount = finalizedStream.readInt();
        for (int i = 0; i < prefixFileCount; ++i) {
            String metadataFilePath =
                finalizedStream.readUTF() + CHECKPOINT_METADATA_POSTFIX;
            validMetadataPathList.add(new Path(metadataFilePath));
        }

        // Set the merged aggregator data if it exists.
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
            String mergedAggregatorPath =
                getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
            if (LOG.isInfoEnabled()) {
                LOG.info("mapFilesToWorkers: Reloading merged aggregator " +
                         "data '" + Arrays.toString(aggregatorZkData) +
                         "' to previous checkpoint in path " +
                         mergedAggregatorPath);
            }
            if (getZkExt().exists(mergedAggregatorPath, false) == null) {
                getZkExt().createExt(mergedAggregatorPath,
                                     aggregatorZkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            }
            else {
                getZkExt().setData(mergedAggregatorPath, aggregatorZkData, -1);
            }
        }
        finalizedStream.close();

        Map<String, Set<String>> workerFileSetMap =
            new HashMap<String, Set<String>>();
        // Reading the metadata files.  For now, implement simple algorithm:
        // 1. Every file is set an input split and the partitions are mapped
        //    accordingly
        // 2. Every worker gets a hint about which files to look in to find
        //    the input splits
        int chosenWorkerListIndex = 0;
        int inputSplitIndex = 0;
        I maxVertexIndex = BspUtils.<I>createVertexIndex(getConfiguration());
        for (Path metadataPath : validMetadataPathList) {
            String checkpointFilePrefix = metadataPath.toString();
            checkpointFilePrefix =
                checkpointFilePrefix.substring(
                0,
                checkpointFilePrefix.length() -
                CHECKPOINT_METADATA_POSTFIX.length());
            DataInputStream metadataStream = fs.open(metadataPath);
            long entries = metadataStream.readLong();
            JSONArray vertexRangeMetaArray = new JSONArray();
            JSONArray vertexRangeArray = new JSONArray();
            String chosenWorker =
                chosenWorkerList.get(chosenWorkerListIndex);
            for (long i = 0; i < entries; ++i) {
                long dataPos = metadataStream.readLong();
                Long vertexCount = metadataStream.readLong();
                Long edgeCount = metadataStream.readLong();
                maxVertexIndex.readFields(metadataStream);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mapFileToWorkers: File " + metadataPath +
                              " with position " + dataPos + ", vertex count " +
                              vertexCount + " assigned to " + chosenWorker);
                }
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                maxVertexIndex.write(output);

                JSONObject vertexRangeObj = new JSONObject();

                try {
                    vertexRangeObj.put(JSONOBJ_NUM_VERTICES_KEY,
                                       vertexCount);
                    vertexRangeObj.put(JSONOBJ_NUM_EDGES_KEY,
                                       edgeCount);
                    vertexRangeObj.put(JSONOBJ_HOSTNAME_ID_KEY,
                                       chosenWorker);
                    vertexRangeObj.put(JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY,
                                       checkpointFilePrefix);
                    vertexRangeObj.put(JSONOBJ_MAX_VERTEX_INDEX_KEY,
                                       Base64.encodeBase64String(
                                           outputStream.toByteArray()));
                    vertexRangeMetaArray.put(vertexRangeObj);
                    vertexRangeArray.put(outputStream.toString("UTF-8"));
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "mapFilesToWorkers: JSONException ", e);
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
            LOG.debug("mapFilesToWorkers: Assigning (if not empty) " +
                     chosenWorker + " the following vertexRanges: " +
                     vertexRangeArray.toString());

            // Assign the input splits if there is at least one vertex range
            if (vertexRangeArray.length() == 0) {
                continue;
            }
            try {
                if (LOG.isInfoEnabled()) {
                    LOG.info("mapFilesToWorkers: vertexRangeMetaArray size=" +
                             vertexRangeMetaArray.toString().length());
                }
                getZkExt().createExt(inputSplitPathFinishedPath,
                                     vertexRangeMetaArray.toString().getBytes(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "mapFilesToWorkers: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "mapFilesToWorkers: IllegalStateException", e);
            }
            ++inputSplitIndex;
            ++chosenWorkerListIndex;
            if (chosenWorkerListIndex == chosenWorkerList.size()) {
                chosenWorkerListIndex = 0;
            }
        }
    }

    public void setup() {
        // Might have to manually load a checkpoint.
        // In that case, the input splits are not set, they will be faked by
        // the checkpoint files.  Each checkpoint file will be an input split
        // and the input split
        superstepCounter = getContext().getCounter(
            "Giraph Stats", "Superstep");
        vertexCounter = getContext().getCounter(
            "Giraph Stats", "Aggregate vertices");
        finishedVertexCounter = getContext().getCounter(
            "Giraph Stats", "Aggregate finished vertices");
        edgeCounter = getContext().getCounter(
            "Giraph Stats", "Aggregate edges");
        currentWorkersCounter = getContext().getCounter(
            "Giraph Stats", "Current workers");
        if (getRestartedSuperstep() == -1) {
            return;
        }
        else if (getRestartedSuperstep() < -1) {
            LOG.fatal("setup: Impossible to restart superstep " +
                      getRestartedSuperstep());
            setJobState(BspService.State.FAILED, -1, -1);
        } else {
            superstepCounter.increment(getRestartedSuperstep());
        }

    }

    public boolean becomeMaster() {
        // Create my bid to become the master, then try to become the worker
        // or return false.
        String myBid = null;
        try {
            myBid =
                getZkExt().createExt(MASTER_ELECTION_PATH +
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
                    State.valueOf(jobState.getString(JSONOBJ_STATE_KEY)) ==
                        State.FINISHED) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("becomeMaster: Job is finished, " +
                                 "give up trying to be the master!");
                    }
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
                        MASTER_ELECTION_PATH, true, true, true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("becomeMaster: First child is '" +
                             masterChildArr.get(0) + "' and my bid is '" +
                             myBid + "'");
                }
                if (masterChildArr.get(0).equals(myBid)) {
                    LOG.info("becomeMaster: I am now the master!");
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
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectVertexRangeStats: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectVertexRangeStats: IllegalStateException", e);
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
                    statArray = new JSONArray(new String(zkData));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("collectVertexRangeStats: input split path " +
                                  inputSplitPath + " got " + statArray);
                    }
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: JSONException", e);
                } catch (KeeperException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: KeeperException", e);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: InterruptedException", e);
                }
                for (int i = 0; i < statArray.length(); ++i) {
                    try {
                        I maxVertexIndex =
                            BspUtils.<I>createVertexIndex(getConfiguration());
                        byte[] maxVertexIndexByteArray =
                            Base64.decodeBase64(
                                statArray.getJSONObject(i).getString(
                                    BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY));
                        InputStream input =
                            new ByteArrayInputStream(maxVertexIndexByteArray);
                        ((Writable) maxVertexIndex).readFields(
                            new DataInputStream(input));
                        statArray.getJSONObject(i).put(
                            JSONOBJ_FINISHED_VERTICES_KEY,
                            0);

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("collectVertexRangeStats: "+
                                      "On input split path " +
                                      inputSplitPath + ", maxVertexIndex = "
                                      + maxVertexIndex +
                                      ", vertex range stats = " +
                                      statArray.getJSONObject(i));
                        }
                        if (vertexRangeStatArrayMap.containsKey(maxVertexIndex)) {
                            throw new IllegalStateException(
                                "collectVertexRangeStats: Already got stats " +
                                "for max vertex index " + maxVertexIndex + " (");
                        }
                        vertexRangeStatArrayMap.put(maxVertexIndex,
                                                    statArray.getJSONObject(i));
                    } catch (JSONException e) {
                        throw new IllegalStateException(
                            "collectVertexRangeStats: JSONException", e);
                    } catch (UnsupportedEncodingException e) {
                        throw new IllegalStateException(
                            "collectVertexRangeStats: " +
                            "UnsupportedEncodingException", e);
                    } catch (IOException e) {
                        throw new IllegalStateException(
                            "collectVertexRangeStats: IOException", e);
                    }
                }
            }
        }
        else {
            String workerFinishedPath =
                getWorkerFinishedPath(getApplicationAttempt(), superstep);
            List<String> workerFinishedPathList = null;
            try {
                workerFinishedPathList =
                    getZkExt().getChildrenExt(
                        workerFinishedPath, false, false, true);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectVertexRangeStats: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectVertexRangeStats: InterruptedException", e);
            }

            for (String finishedPath : workerFinishedPathList) {
                JSONObject aggregatorStatObj= null;
                try {
                    byte [] zkData =
                        getZkExt().getData(finishedPath, false, null);
                    aggregatorStatObj = new JSONObject(new String(zkData));

                    JSONArray statArray =
                        aggregatorStatObj.getJSONArray(
                            JSONOBJ_VERTEX_RANGE_STAT_ARRAY_KEY);
                    for (int i = 0; i < statArray.length(); ++i) {
                        I maxVertexIndex =
                            BspUtils.<I>createVertexIndex(getConfiguration());
                        byte[] maxVertexIndexByteArray =
                            Base64.decodeBase64(
                                statArray.getJSONObject(i).getString(
                                    BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY));
                        InputStream input =
                            new ByteArrayInputStream(maxVertexIndexByteArray);
                        ((Writable) maxVertexIndex).readFields(
                            new DataInputStream(input));
                        vertexRangeStatArrayMap.put(maxVertexIndex,
                                                    statArray.getJSONObject(i));
                    }
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: JSONException", e);
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: " +
                        "UnsupportedEncodingException", e);
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: IOException", e);
                } catch (KeeperException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: KeeperException", e);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(
                        "collectVertexRangeStats: InterruptedException", e);
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
        if (superstep <= 0) {
            return;
        }
        Map<String, Aggregator<? extends Writable>> aggregatorMap =
            new TreeMap<String, Aggregator<? extends Writable>>();
        String workerFinishedPath =
            getWorkerFinishedPath(getApplicationAttempt(), superstep);
        List<String> hostnameIdPathList = null;
        try {
            hostnameIdPathList =
                getZkExt().getChildrenExt(
                    workerFinishedPath, false, false, true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "collectAndProcessAggregatorValues: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "collectAndProcessAggregatorValues: InterruptedException", e);
        }

        for (String hostnameIdPath : hostnameIdPathList) {
            JSONObject aggregatorsStatObj = null;
            JSONArray aggregatorArray = null;
            try {
                byte [] zkData =
                    getZkExt().getData(hostnameIdPath, false, null);
                aggregatorsStatObj = new JSONObject(new String(zkData));
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: InterruptedException",
                    e);
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: JSONException", e);
            }
            try {
                aggregatorArray = aggregatorsStatObj.getJSONArray(
                    JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY);
            } catch (JSONException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("collectAndProcessAggregatorValues: " +
                              "No aggregators" + " for " + hostnameIdPath);
                }
                continue;
            }
            for (int i = 0; i < aggregatorArray.length(); ++i) {
                try {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("collectAndProcessAggregatorValues: " +
                                 "Getting aggregators from " +
                                 aggregatorArray.getJSONObject(i));
                    }
                    String aggregatorName =
                        aggregatorArray.getJSONObject(i).getString(
                            AGGREGATOR_NAME_KEY);
                    String aggregatorClassName =
                        aggregatorArray.getJSONObject(i).getString(
                            AGGREGATOR_CLASS_NAME_KEY);
                    @SuppressWarnings("unchecked")
                    Aggregator<Writable> aggregator =
                        (Aggregator<Writable>) aggregatorMap.get(aggregatorName);
                    boolean firstTime = false;
                    if (aggregator == null) {
                        @SuppressWarnings("unchecked")
                        Aggregator<Writable> aggregatorWritable =
                            (Aggregator<Writable>) getAggregator(aggregatorName);
                        aggregator = aggregatorWritable;
                        if (aggregator == null) {
                            @SuppressWarnings("unchecked")
                            Class<? extends Aggregator<Writable>> aggregatorClass =
                                (Class<? extends Aggregator<Writable>>)
                                    Class.forName(aggregatorClassName);
                            aggregator = registerAggregator(
                                aggregatorName,
                                aggregatorClass);
                        }
                        aggregatorMap.put(aggregatorName, aggregator);
                        firstTime = true;
                    }
                    Writable aggregatorValue =
                        aggregator.createAggregatedValue();
                    InputStream input =
                        new ByteArrayInputStream(
                            Base64.decodeBase64(
                                aggregatorArray.getJSONObject(i).
                                getString(AGGREGATOR_VALUE_KEY)));
                    aggregatorValue.readFields(new DataInputStream(input));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("collectAndProcessAggregatorValues: " +
                                  "aggregator value size=" + input.available() +
                                  " for aggregator=" + aggregatorName +
                                  " value=" + aggregatorValue);
                    }
                    if (firstTime) {
                        aggregator.setAggregatedValue(aggregatorValue);
                    } else {
                        aggregator.aggregate(aggregatorValue);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IOException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "JSONException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "ClassNotFoundException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (InstantiationException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "InstantiationException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IOException when reading aggregator data " +
                        aggregatorArray, e);
                }
            }
        }
        if (aggregatorMap.size() > 0) {
            String mergedAggregatorPath =
                getMergedAggregatorPath(getApplicationAttempt(), superstep);
            byte [] zkData = null;
            JSONArray aggregatorArray = new JSONArray();
            for (Map.Entry<String, Aggregator<? extends Writable>> entry :
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
                        Base64.encodeBase64String(outputStream.toByteArray()));
                    aggregatorArray.put(aggregatorObj);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("collectAndProcessAggregatorValues: " +
                                 "Trying to add aggregatorObj " +
                                 aggregatorObj + "(" +
                                 entry.getValue().getAggregatedValue() +
                                 ") to merged aggregator path " +
                                 mergedAggregatorPath);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IllegalStateException", e);
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: JSONException", e);
                }
            }
            try {
                zkData = aggregatorArray.toString().getBytes();
                getZkExt().createExt(mergedAggregatorPath,
                                     zkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("collectAndProcessAggregatorValues: " +
                         mergedAggregatorPath+
                         " already exists!");
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: IllegalStateException",
                    e);
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("collectAndProcessAggregatorValues: Finished " +
                         "loading " +
                         mergedAggregatorPath+ " with aggregator values " +
                         aggregatorArray);
            }
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
        String mergedAggregatorPath =
            getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
        if (getZkExt().exists(mergedAggregatorPath, false) != null) {
            byte [] aggregatorZkData =
                getZkExt().getData(mergedAggregatorPath, false, null);
            finalizedOutputStream.writeInt(aggregatorZkData.length);
            finalizedOutputStream.write(aggregatorZkData);
        }
        else {
            finalizedOutputStream.writeInt(0);
        }
        finalizedOutputStream.close();
        lastCheckpointedSuperstep = superstep;
    }

    /**
     * Convert the processed input split data into vertex range assignments
     * that can be used on the next superstep.
     *
     * @param chosenWorkerHostnamePortMap map of chosen workers to
     *        a hostname and port array
     */
    private void inputSplitsToVertexRanges(
            Map<String, JSONArray> chosenWorkerHostnamePortMap) {
        List<String> inputSplitPathList = null;
        List<VertexRange<I, V, E, M>> vertexRangeList =
            new ArrayList<VertexRange<I, V, E, M>>();
        try {
            inputSplitPathList = getZkExt().getChildrenExt(INPUT_SPLIT_PATH,
                                                           false,
                                                           false,
                                                           true);
            int numRanges = 0;
            for (String inputSplitPath : inputSplitPathList) {
                String inputSplitFinishedPath = inputSplitPath +
                    INPUT_SPLIT_FINISHED_NODE;
                byte [] zkData =
                    getZkExt().getData(inputSplitFinishedPath, false, null);
                if (zkData == null || zkData.length == 0) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("inputSplitsToVertexRanges: No vertex ranges " +
                                 "in " + inputSplitFinishedPath);
                    }
                    continue;
                }
                JSONArray statArray = new JSONArray(new String(zkData));
                JSONObject vertexRangeObj = null;
                for (int i = 0; i < statArray.length(); ++i) {
                    vertexRangeObj = statArray.getJSONObject(i);
                    String hostnamePartitionId =
                        vertexRangeObj.getString(JSONOBJ_HOSTNAME_ID_KEY);
                    if (!chosenWorkerHostnamePortMap.containsKey(
                        hostnamePartitionId)) {
                        LOG.fatal("inputSplitsToVertexRanges: Worker " +
                              hostnamePartitionId + " died, failing.");
                        setJobState(State.FAILED, -1, -1);
                    }
                   JSONArray hostnamePortArray =
                       chosenWorkerHostnamePortMap.get(hostnamePartitionId);
                   vertexRangeObj.put(JSONOBJ_HOSTNAME_KEY,
                                      hostnamePortArray.getString(0));
                   vertexRangeObj.put(JSONOBJ_PORT_KEY,
                                      hostnamePortArray.getString(1));
                   Class<I> indexClass =
                       BspUtils.getVertexIndexClass(getConfiguration());
                    VertexRange<I, V, E, M> vertexRange =
                        new VertexRange<I, V, E, M>(indexClass, vertexRangeObj);
                    vertexRangeList.add(vertexRange);
                    ++numRanges;
                }
            }

            JSONArray vertexRangeAssignmentArray =
                new JSONArray();
            for (VertexRange<I, V, E, M> vertexRange : vertexRangeList) {
                vertexRangeAssignmentArray.put(vertexRange.toJSONObject());
            }
            String vertexRangeAssignmentsPath =
                getVertexRangeAssignmentsPath(getApplicationAttempt(),
                                              getSuperstep());
            if (LOG.isInfoEnabled()) {
                LOG.info("inputSplitsToVertexRanges: Assigning " + numRanges +
                         " vertex ranges of total length " +
                         vertexRangeAssignmentArray.toString().length() +
                         " to path " + vertexRangeAssignmentsPath);
            }
            getZkExt().createExt(vertexRangeAssignmentsPath,
                                 vertexRangeAssignmentArray.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: IllegalStateException", e);
        } catch (JSONException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: JSONException", e);
        } catch (IOException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: IOException", e);
        } catch (InstantiationException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: InstantiationException", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                "inputSplitsToVertexRanges: IllegalAccessException", e);
        }
    }

    /**
     * Balance the vertex ranges before the next superstep computation begins.
     * Wait until the vertex ranges data has been moved to the correct
     * worker prior to continuing.
     *
     * @param vertexRangeBalancer balancer to use
     * @param chosenWorkerHostnamePortMap workers available
     * @throws JSONException
     * @throws IOException
     * @throws UnsupportedEncodingException
     */
    private void balanceVertexRanges(
            VertexRangeBalancer<I, V, E, M> vertexRangeBalancer,
            Map<String, JSONArray> chosenWorkerHostnamePortMap) {
        vertexRangeBalancer.setSuperstep(getSuperstep());
        vertexRangeBalancer.setWorkerHostnamePortMap(
            chosenWorkerHostnamePortMap);
        NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap =
            getVertexRangeMap(getSuperstep() - 1);
        vertexRangeBalancer.setPrevVertexRangeMap(vertexRangeMap);
        NavigableMap<I, VertexRange<I, V, E, M>> nextVertexRangeMap =
            vertexRangeBalancer.rebalance();
        vertexRangeBalancer.setNextVertexRangeMap(nextVertexRangeMap);
        vertexRangeBalancer.setPreviousHostnamePort();
        nextVertexRangeMap = vertexRangeBalancer.getNextVertexRangeMap();
        if (nextVertexRangeMap.size() != vertexRangeMap.size()) {
            throw new IllegalArgumentException(
                "balanceVertexRanges: Next vertex range count " +
                nextVertexRangeMap.size() + " != " + vertexRangeMap.size());
        }
        JSONArray vertexRangeAssignmentArray = new JSONArray();
        VertexRange<I, V, E, M> maxVertexRange = null; // TODO: delete after Bug 4340282 fixed.
        for (VertexRange<I, V, E, M> vertexRange :
                nextVertexRangeMap.values()) {
            try {
                vertexRangeAssignmentArray.put(
                    vertexRange.toJSONObject());
                if (maxVertexRange == null || // TODO: delete after Bug 4340282 fixed.
                        vertexRange.toJSONObject().toString().length() <
                        maxVertexRange.toJSONObject().toString().length()) {
                    maxVertexRange = vertexRange;
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                    "balanceVertexRanges: IOException", e);
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "balanceVertexRanges: JSONException", e);
            }
        }
        if (vertexRangeAssignmentArray.length() == 0) {
            throw new RuntimeException(
                "balanceVertexRanges: Impossible there are no vertex ranges ");
        }

        byte[] vertexAssignmentBytes = null;
        try {
            vertexAssignmentBytes =
                vertexRangeAssignmentArray.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                "balanceVertexranges: Cannot get bytes from " +
                vertexRangeAssignmentArray.toString(), e);
        }

        // TODO: delete after Bug 4340282 fixed.
        if (LOG.isInfoEnabled()) {
            try {
                LOG.info("balanceVertexRanges: numVertexRanges=" +
                         nextVertexRangeMap.values().size() +
                         " lengthVertexRanges=" + vertexAssignmentBytes.length +
                         " maxVertexRangeLength=" +
                         maxVertexRange.toJSONObject().toString().length());
            } catch (IOException e) {
                throw new RuntimeException(
                    "balanceVertexRanges: IOException fetching " +
                    maxVertexRange.toString(), e);
            } catch (JSONException e) {
                throw new RuntimeException(
                    "balanceVertexRanges: JSONException fetching " +
                    maxVertexRange.toString(), e);
            }
        }

        String vertexRangeAssignmentsPath =
            getVertexRangeAssignmentsPath(getApplicationAttempt(),
                                          getSuperstep());
        try {
            getZkExt().createExt(vertexRangeAssignmentsPath,
                                 vertexAssignmentBytes,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("balanceVertexRanges: Node " +
                     vertexRangeAssignmentsPath + " already exists", e);
            }
        } catch (KeeperException e) {
            LOG.fatal("balanceVertexRanges: Got KeeperException", e);
            throw new IllegalStateException(
                "balanceVertexRanges: Got KeeperException", e);
        } catch (InterruptedException e) {
            LOG.fatal("balanceVertexRanges: Got InterruptedException", e);
            throw new IllegalStateException(
                "balanceVertexRanges: Got InterruptedException", e);
        }

        long changes = vertexRangeBalancer.getVertexRangeChanges();
        if (LOG.isInfoEnabled()) {
            LOG.info("balanceVertexRanges: Waiting on " + changes +
                     " vertex range changes");
        }
        if (changes == 0) {
            return;
        }

        String vertexRangeExchangePath =
            getVertexRangeExchangePath(getApplicationAttempt(), getSuperstep());
        List<String> workerExchangeList = null;
        try {
            getZkExt().createExt(vertexRangeExchangePath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("balanceVertexRanges: " + vertexRangeExchangePath +
                         "exists");
            }
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "balanceVertexRanges: Got KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "balanceVertexRanges: Got InterruptedException", e);
        }
        while (true) {
            try {
                workerExchangeList =
                    getZkExt().getChildrenExt(
                        vertexRangeExchangePath, true, false, false);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "balanceVertexRanges: Got KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "balanceVertexRanges: Got InterruptedException", e);
            }
            if (workerExchangeList.size() == changes) {
                break;
            }
            getVertexRangeExchangeChildrenChangedEvent().waitForever();
            getVertexRangeExchangeChildrenChangedEvent().reset();
        }
        String vertexRangeExchangeFinishedPath =
            getVertexRangeExchangeFinishedPath(getApplicationAttempt(),
                                               getSuperstep());
        try {
            getZkExt().createExt(vertexRangeExchangeFinishedPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "balanceVertexRanges: Got KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "balanceVertexRanges: Got InterruptedException", e);
        }
    }

    /**
     * Check whether the workers chosen for this superstep are still alive
     *
     * @param chosenWorkerHealthPath Path to the healthy workers in ZooKeeper
     * @param chosenWorkerSet Set of the healthy workers
     * @return true if they are all alive, false otherwise.
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean superstepChosenWorkerAlive(
            String chosenWorkerHealthPath,
            Set<String> chosenWorkerSet)
            throws KeeperException, InterruptedException {
        List<String> chosenWorkerHealthyList =
            getZkExt().getChildrenExt(chosenWorkerHealthPath,
                                      true,
                                      false,
                                      false);
        Set<String> chosenWorkerHealthySet =
            new HashSet<String>(chosenWorkerHealthyList);
        boolean allChosenWorkersHealthy = true;
        for (String chosenWorker : chosenWorkerSet) {
            if (!chosenWorkerHealthySet.contains(chosenWorker)) {
                allChosenWorkersHealthy = false;
                LOG.warn("superstepChosenWorkerAlive: Missing chosen worker " +
                         chosenWorker + " on superstep " + getSuperstep());
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
            getZkExt().deleteExt(INPUT_SPLIT_PATH, -1, true);
        } catch (InterruptedException e) {
            throw new RuntimeException(
                "retartFromCheckpoint: InterruptedException", e);
        } catch (KeeperException e) {
            throw new RuntimeException(
                "retartFromCheckpoint: KeeperException", e);
        }
        setApplicationAttempt(getApplicationAttempt() + 1);
        setCachedSuperstep(checkpoint);
        setRestartedSuperstep(checkpoint);
        setJobState(BspService.State.START_SUPERSTEP,
                    getApplicationAttempt(),
                    checkpoint);
    }

    /**
     * Only get the finalized checkpoint files
     */
    public static class FinalizedCheckpointPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            if (path.getName().endsWith(
                    BspService.CHECKPOINT_FINALIZED_POSTFIX)) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public long getLastGoodCheckpoint() throws IOException {
        // Find the last good checkpoint if none have been written to the
        // knowledge of this master
        if (lastCheckpointedSuperstep == -1) {
            FileStatus[] fileStatusArray =
                getFs().listStatus(new Path(CHECKPOINT_BASE_PATH),
                                   new FinalizedCheckpointPathFilter());
            if (fileStatusArray == null) {
                return -1;
            }
            Arrays.sort(fileStatusArray);
            lastCheckpointedSuperstep = getCheckpoint(
                fileStatusArray[fileStatusArray.length - 1].getPath());
            LOG.info("getLastGoodCheckpoint: Found last good checkpoint " +
                     lastCheckpointedSuperstep + " from " +
                     fileStatusArray[fileStatusArray.length - 1].
                     getPath().toString());
        }
        return lastCheckpointedSuperstep;
    }

    @Override
    public SuperstepState coordinateSuperstep() throws
            KeeperException, InterruptedException {
        // 1. Get chosen workers and set up watches on them.
        // 2. Assign partitions to the workers
        //    or possibly reload from a superstep
        // 3. Wait for all workers to complete
        // 4. Collect and process aggregators
        // 5. Create superstep finished node
        // 6. If the checkpoint frequency is met, finalize the checkpoint
        Map<String, JSONArray> chosenWorkerHostnamePortMap = checkWorkers();
        if (chosenWorkerHostnamePortMap == null) {
            LOG.fatal("coordinateSuperstep: Not enough healthy workers for " +
                      "superstep " + getSuperstep());
            setJobState(State.FAILED, -1, -1);
        } else {
            for (Entry<String, JSONArray> entry :
                    chosenWorkerHostnamePortMap.entrySet()) {
                String workerHealthyPath =
                    getWorkerHealthyPath(getApplicationAttempt(),
                                         getSuperstep()) + "/" + entry.getKey();
                if (getZkExt().exists(workerHealthyPath, true) == null) {
                    LOG.warn("coordinateSuperstep: Chosen worker " +
                             workerHealthyPath +
                             " is no longer valid, failing superstep");
                }
            }
        }

        currentWorkersCounter.increment(chosenWorkerHostnamePortMap.size() -
                                        currentWorkersCounter.getValue());
        if (getRestartedSuperstep() == getSuperstep()) {
            try {
                if (LOG.isInfoEnabled()) {
                    LOG.info("coordinateSuperstep: Reloading from superstep " +
                             getSuperstep());
                }
                mapFilesToWorkers(
                    getRestartedSuperstep(),
                    new ArrayList<String>(
                        chosenWorkerHostnamePortMap.keySet()));
                inputSplitsToVertexRanges(chosenWorkerHostnamePortMap);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: Failed to reload", e);
            }
        } else {
            if (getSuperstep() > 0) {
                VertexRangeBalancer<I, V, E, M> vertexRangeBalancer =
                    BspUtils.<I, V, E, M>createVertexRangeBalancer(
                        getConfiguration());
                synchronized (vertexRangeSynchronization) {
                    balanceVertexRanges(vertexRangeBalancer,
                                        chosenWorkerHostnamePortMap);
                }
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("coordinateSuperstep: finishedWorkers " +
                          finishedWorkerPath +
                          " already exists, no need to create");
            }
        }
        String workerHealthyPath =
            getWorkerHealthyPath(getApplicationAttempt(), getSuperstep());
        List<String> finishedWorkerList = null;
        long nextInfoMillis = System.currentTimeMillis();
        while (true) {
            try {
                finishedWorkerList =
                    getZkExt().getChildrenExt(finishedWorkerPath,
                                              true,
                                              false,
                                              false);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: Couldn't get children of " +
                    finishedWorkerPath, e);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("coordinateSuperstep: Got finished worker list = " +
                          finishedWorkerList + ", size = " +
                          finishedWorkerList.size() +
                          ", chosen worker list = " +
                          chosenWorkerHostnamePortMap.keySet() + ", size = " +
                          chosenWorkerHostnamePortMap.size() +
                          " from " + finishedWorkerPath);
            }

            if (LOG.isInfoEnabled() &&
                    (System.currentTimeMillis() > nextInfoMillis)) {
                nextInfoMillis = System.currentTimeMillis() + 30000;
                LOG.info("coordinateSuperstep: " + finishedWorkerList.size() +
                         " out of " +
                         chosenWorkerHostnamePortMap.size() +
                         " chosen workers finished on superstep " +
                         getSuperstep());
            }
            getContext().setStatus(getBspMapper().getMapFunctions() + " " +
                                   finishedWorkerList.size() +
                                   " finished out of " +
                                   chosenWorkerHostnamePortMap.size() +
                                   " on superstep " + getSuperstep());
            if (finishedWorkerList.containsAll(
                chosenWorkerHostnamePortMap.keySet())) {
                break;
            }
            getSuperstepStateChangedEvent().waitForever();
            getSuperstepStateChangedEvent().reset();

            // Did a worker die?
            if ((getSuperstep() > 0) &&
                    !superstepChosenWorkerAlive(
                        workerHealthyPath,
                        chosenWorkerHostnamePortMap.keySet())) {
                return SuperstepState.WORKER_FAILURE;
            }
        }
        collectAndProcessAggregatorValues(getSuperstep());
        Map<I, JSONObject> maxIndexStatsMap =
            collectVertexRangeStats(getSuperstep());
        long verticesFinished = 0;
        long verticesTotal = 0;
        long edgesTotal = 0;
        for (Map.Entry<I, JSONObject> entry : maxIndexStatsMap.entrySet()) {
            try {
                verticesFinished +=
                    entry.getValue().getLong(JSONOBJ_FINISHED_VERTICES_KEY);
                verticesTotal +=
                    entry.getValue().getLong(JSONOBJ_NUM_VERTICES_KEY);
                edgesTotal +=
                    entry.getValue().getLong(JSONOBJ_NUM_EDGES_KEY);
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: Failed to parse out "+
                    "the number range stats", e);
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("coordinateSuperstep: Aggregation found "
                     + verticesFinished + " of " + verticesTotal +
                     " vertices finished, " + edgesTotal +
                     " edges on superstep = " + getSuperstep());
        }

        // Convert the input split stats to vertex ranges in superstep 0
        if (getSuperstep() == 0) {
            inputSplitsToVertexRanges(chosenWorkerHostnamePortMap);
        }

        // Let everyone know the aggregated application state through the
        // superstep
        String superstepFinishedNode =
            getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
        try {
            JSONObject globalInfoObject = new JSONObject();
            globalInfoObject.put(JSONOBJ_FINISHED_VERTICES_KEY,
                                 verticesFinished);
            globalInfoObject.put(JSONOBJ_NUM_VERTICES_KEY, verticesTotal);
            globalInfoObject.put(JSONOBJ_NUM_EDGES_KEY, edgesTotal);
            getZkExt().createExt(superstepFinishedNode,
                                 globalInfoObject.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (JSONException e) {
            throw new IllegalStateException("coordinateSuperstep: " +
                                            "JSONException", e);
        }
        vertexCounter.increment(verticesTotal -
                                vertexCounter.getValue());
        finishedVertexCounter.increment(verticesFinished -
                                        finishedVertexCounter.getValue());
        edgeCounter.increment(edgesTotal - edgeCounter.getValue());

        // Finalize the valid checkpoint file prefixes and possibly
        // the aggregators.
        if (checkpointFrequencyMet(getSuperstep())) {
            try {
                finalizeCheckpoint(
                    getSuperstep(),
                    new ArrayList<String>(chosenWorkerHostnamePortMap.keySet()));
            } catch (IOException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: IOException on finalizing checkpoint",
                    e);
            }
        }

        // Clean up the old supersteps (always keep this one)
        long removeableSuperstep = getSuperstep() - 1;
        if ((getConfiguration().getBoolean(
                GiraphJob.KEEP_ZOOKEEPER_DATA,
                GiraphJob.KEEP_ZOOKEEPER_DATA_DEFAULT) == false) &&
            (removeableSuperstep >= 0)) {
            String oldSuperstepPath =
                getSuperstepPath(getApplicationAttempt()) + "/" +
                (removeableSuperstep);
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
     incrCachedSuperstep();
        superstepCounter.increment(1);
        return (verticesFinished == verticesTotal) ?
            SuperstepState.ALL_SUPERSTEPS_DONE :
            SuperstepState.THIS_SUPERSTEP_DONE;
    }

    /**
     * Need to clean up ZooKeeper nicely.  Make sure all the masters and workers
     * have reported ending their ZooKeeper connections.
     */
    private void cleanUpZooKeeper() {
        try {
            getZkExt().createExt(CLEANED_UP_PATH,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanUpZooKeeper: Node " + CLEANED_UP_PATH +
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
        if (getBspMapper().getMapFunctions() == MapFunctions.ALL) {
            maxTasks *= 2;
        }
        List<String> cleanedUpChildrenList = null;
        while (true) {
            try {
                cleanedUpChildrenList =
                    getZkExt().getChildrenExt(
                        CLEANED_UP_PATH, true, false, true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanUpZooKeeper: Got " +
                             cleanedUpChildrenList.size() + " of " +
                             maxTasks  +  " desired children from " +
                             CLEANED_UP_PATH);
                }
                if (cleanedUpChildrenList.size() == maxTasks) {
                    break;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanedUpZooKeeper: Waiting for the " +
                             "children of " + CLEANED_UP_PATH +
                             " to change since only got " +
                             cleanedUpChildrenList.size() + " nodes.");
                }
            }
            catch (Exception e) {
                // We are in the cleanup phase -- just log the error
                LOG.error("cleanUpZooKeeper: Got exception, but will continue",
                          e);
                return;
            }

            getCleanedUpChildrenChangedEvent().waitForever();
            getCleanedUpChildrenChangedEvent().reset();
        }

         // At this point, all processes have acknowledged the cleanup,
         // and the master can do any final cleanup
        try {
            if (getConfiguration().getBoolean(
                GiraphJob.KEEP_ZOOKEEPER_DATA,
                GiraphJob.KEEP_ZOOKEEPER_DATA_DEFAULT) == false) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanupZooKeeper: Removing the following path " +
                             "and all children - " + BASE_PATH);
                }
                getZkExt().deleteExt(BASE_PATH, -1, true);
            }
        } catch (Exception e) {
            LOG.error("cleanupZooKeeper: Failed to do cleanup of " +
                      BASE_PATH, e);
        }
    }

    @Override
    public void cleanup() throws IOException {
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
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanup: Notifying master its okay to cleanup with " +
                         finalFinishedPath);
            }
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanup: Couldn't create finished node '" +
                         cleanedUpPath);
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
                    GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS,
                    GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT)) {
                boolean success =
                    getFs().delete(new Path(CHECKPOINT_BASE_PATH), true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanup: Removed HDFS checkpoint directory (" +
                             CHECKPOINT_BASE_PATH + ") with return = " +
                             success + " since this job succeeded ");
                }
            }
        }

        try {
            getZkExt().close();
        } catch (InterruptedException e) {
            // cleanup phase -- just log the error
            LOG.error("cleanup: Zookeeper failed to close", e);
        }
    }

    /**
     * Event that the master watches that denotes if a worker has done something
     * that changes the state of a superstep (either a worker completed or died)
     *
     * @return Event that denotes a superstep state change
     */
    final public BspEvent getSuperstepStateChangedEvent() {
        return superstepStateChanged;
    }

    /**
     * Should this worker failure cause the current superstep to fail?
     *
     * @param failedWorkerPath Full path to the failed worker
     */
    final private void checkHealthyWorkerFailure(String failedWorkerPath) {
        synchronized(vertexRangeSynchronization) {
            if (getSuperstepFromPath(failedWorkerPath) < getSuperstep()) {
                return;
            }

            // Check all the workers used in this superstep
            NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap =
                getVertexRangeMap(getSuperstep());
            String hostnameId =
                getHealthyHostnameIdFromPath(failedWorkerPath);
            for (VertexRange<I, V, E, M> vertexRange :
                    vertexRangeMap.values()) {
                if (vertexRange.getHostnameId().equals(hostnameId) ||
                        ((vertexRange.getPreviousHostnameId() != null) &&
                                vertexRange.getPreviousHostnameId().equals(
                                    hostnameId))) {
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
        }

        return foundEvent;
    }
}
