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

import net.iharder.Base64;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.RPCCommunications;
import org.apache.giraph.comm.ServerInterface;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionExchange;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.WorkerGraphPartitioner;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceWorker}.
 */
@SuppressWarnings("rawtypes")
public class BspServiceWorker<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends BspService<I, V, E, M>
        implements CentralizedServiceWorker<I, V, E, M> {
    /** Number of input splits */
    private int inputSplitCount = -1;
    /** My process health znode */
    private String myHealthZnode;
    /** List of aggregators currently in use */
    private Set<String> aggregatorInUse = new TreeSet<String>();
    /** Worker info */
    private final WorkerInfo workerInfo;
    /** Worker graph partitioner */
    private final WorkerGraphPartitioner<I, V, E, M> workerGraphPartitioner;
    /** Input split vertex cache (only used when loading from input split) */
    private final Map<PartitionOwner, Partition<I, V, E, M>>
        inputSplitCache = new HashMap<PartitionOwner, Partition<I, V, E, M>>();
    /** Communication service */
    private final ServerInterface<I, V, E, M> commService;
    /** Structure to store the partitions on this worker */
    private final Map<Integer, Partition<I, V, E, M>> workerPartitionMap =
        new HashMap<Integer, Partition<I, V, E, M>>();
    /** Have the partition exchange children (workers) changed? */
    private final BspEvent partitionExchangeChildrenChanged =
        new PredicateLock();
    /** Max vertices per partition before sending */
    private final int maxVerticesPerPartition;
    /** Worker Context */
    private final WorkerContext workerContext;
    /** Total vertices loaded */
    private long totalVerticesLoaded = 0;
    /** Total edges loaded */
    private long totalEdgesLoaded = 0;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);

    public BspServiceWorker(
            String serverPortList,
            int sessionMsecTimeout,
            Mapper<?, ?, ?, ?>.Context context,
            GraphMapper<I, V, E, M> graphMapper,
            GraphState<I, V, E,M> graphState)
            throws UnknownHostException, IOException, InterruptedException {
        super(serverPortList, sessionMsecTimeout, context, graphMapper);
        int finalRpcPort =
            getConfiguration().getInt(GiraphJob.RPC_INITIAL_PORT,
                                      GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
                                      getTaskPartition();
        maxVerticesPerPartition=
            getConfiguration().getInt(
                GiraphJob.MAX_VERTICES_PER_PARTITION,
                GiraphJob.MAX_VERTICES_PER_PARTITION_DEFAULT);
        workerInfo =
            new WorkerInfo(getHostname(), getTaskPartition(), finalRpcPort);
        workerGraphPartitioner =
            getGraphPartitionerFactory().createWorkerGraphPartitioner();
        commService = new RPCCommunications<I, V, E, M>(
            context, this, graphState);
        graphState.setWorkerCommunications(commService);
        this.workerContext = BspUtils.createWorkerContext(getConfiguration(),
            graphMapper.getGraphState());
    }

    public WorkerContext getWorkerContext() {
    	return workerContext;
    }

    /**
     * Intended to check the health of the node.  For instance, can it ssh,
     * dmesg, etc. For now, does nothing.
     */
    public boolean isHealthy() {
        return true;
    }

    /**
     * Use an aggregator in this superstep.
     *
     * @param name
     * @return boolean (false when aggregator not registered)
     */
    public boolean useAggregator(String name) {
        if (getAggregatorMap().get(name) == null) {
            LOG.error("userAggregator: Aggregator=" + name + " not registered");
            return false;
        }
        aggregatorInUse.add(name);
        return true;
    }

    /**
     * Try to reserve an InputSplit for loading.  While InputSplits exists that
     * are not finished, wait until they are.
     *
     * @return reserved InputSplit or null if no unfinished InputSplits exist
     */
    private String reserveInputSplit() {
        List<String> inputSplitPathList = null;
        try {
            inputSplitPathList =
                getZkExt().getChildrenExt(INPUT_SPLIT_PATH, false, false, true);
            if (inputSplitCount == -1) {
                inputSplitCount = inputSplitPathList.size();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String reservedInputSplitPath = null;
        Stat reservedStat = null;
        while (true) {
            int finishedInputSplits = 0;
            for (String inputSplitPath : inputSplitPathList) {
                String tmpInputSplitFinishedPath =
                    inputSplitPath + INPUT_SPLIT_FINISHED_NODE;
                try {
                    reservedStat =
                        getZkExt().exists(tmpInputSplitFinishedPath, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (reservedStat != null) {
                    ++finishedInputSplits;
                    continue;
                }

                String tmpInputSplitReservedPath =
                    inputSplitPath + INPUT_SPLIT_RESERVED_NODE;
                try {
                    reservedStat =
                        getZkExt().exists(tmpInputSplitReservedPath, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (reservedStat == null) {
                    try {
                        // Attempt to reserve this InputSplit
                        getZkExt().createExt(tmpInputSplitReservedPath,
                                       null,
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.EPHEMERAL,
                                       false);
                        reservedInputSplitPath = inputSplitPath;
                        if (LOG.isInfoEnabled()) {
                            LOG.info("reserveInputSplit: Reserved input " +
                                     "split path " + reservedInputSplitPath);
                        }
                        return reservedInputSplitPath;
                    } catch (KeeperException.NodeExistsException e) {
                        LOG.info("reserveInputSplit: Couldn't reserve (already " +
                                 "reserved) inputSplit" +
                                 " at " + tmpInputSplitReservedPath);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("reserveInputSplit: reservedPath = " +
                         reservedInputSplitPath + ", " + finishedInputSplits +
                         " of " + inputSplitPathList.size() +
                         " InputSplits are finished.");
            }
            if (finishedInputSplits == inputSplitPathList.size()) {
                return null;
            }
            // Wait for either a reservation to go away or a notification that
            // an InputSplit has finished.
            getInputSplitsStateChangedEvent().waitForever();
            getInputSplitsStateChangedEvent().reset();
        }
    }

    /**
     * Load the vertices from the user-defined VertexReader into our partitions
     * of vertex ranges.  Do this until all the InputSplits have been processed.
     * All workers will try to do as many InputSplits as they can.  The master
     * will monitor progress and stop this once all the InputSplits have been
     * loaded and check-pointed.
     *
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private VertexEdgeCount loadVertices() throws IOException, ClassNotFoundException,
            InterruptedException, InstantiationException,
            IllegalAccessException {
        String inputSplitPath = null;
        VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
        while ((inputSplitPath = reserveInputSplit()) != null) {
            vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(
                loadVerticesFromInputSplit(inputSplitPath));
        }
        return vertexEdgeCount;
    }

    /**
     * Extract vertices from input split, saving them into a mini cache of
     * partitions.  Periodically flush the cache of vertices when a limit is
     * reached.  Mark the input split finished when done.
     *
     * @param inputSplitPath ZK location of input split
     * @return Mapping of vertex indices and statistics, or null if no data read
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private VertexEdgeCount loadVerticesFromInputSplit(String inputSplitPath)
        throws IOException, ClassNotFoundException, InterruptedException,
               InstantiationException, IllegalAccessException {
        InputSplit inputSplit = getInputSplitForVertices(inputSplitPath);
        VertexEdgeCount vertexEdgeCount =
            readVerticesFromInputSplit(inputSplit);

        // Flush the remaining cached vertices
        for (Entry<PartitionOwner, Partition<I, V, E, M>> entry :
                inputSplitCache.entrySet()) {
            if (!entry.getValue().getVertices().isEmpty()) {
                commService.sendPartitionReq(entry.getKey().getWorkerInfo(),
                                             entry.getValue());
                entry.getValue().getVertices().clear();
            }
        }
        inputSplitCache.clear();

        // Mark this input split done to the master
        String inputSplitFinishedPath =
            inputSplitPath + INPUT_SPLIT_FINISHED_NODE;
        try {
            getZkExt().createExt(inputSplitFinishedPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("loadVerticesFromInputSplit: " + inputSplitFinishedPath +
                     " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("loadVerticesFromInputSplit: Finished loading " +
                     inputSplitPath + " " + vertexEdgeCount);
        }
        return vertexEdgeCount;
    }

    /**
     * Talk to ZooKeeper to convert the input split path to the actual
     * InputSplit containing the vertices to read.
     *
     * @param inputSplitPath Location in ZK of input split
     * @return instance of InputSplit containing vertices to read
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private InputSplit getInputSplitForVertices(String inputSplitPath)
        throws IOException, ClassNotFoundException {
        byte[] splitList;
        try {
            splitList = getZkExt().getData(inputSplitPath, false, null);
        } catch (KeeperException e) {
            throw new IllegalStateException(
               "loadVertices: KeeperException on " + inputSplitPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "loadVertices: IllegalStateException on " + inputSplitPath, e);
        }
        getContext().progress();

        DataInputStream inputStream =
            new DataInputStream(new ByteArrayInputStream(splitList));
        String inputSplitClass = Text.readString(inputStream);
        InputSplit inputSplit = (InputSplit)
            ReflectionUtils.newInstance(
                getConfiguration().getClassByName(inputSplitClass),
                getConfiguration());
        ((Writable) inputSplit).readFields(inputStream);

        if (LOG.isInfoEnabled()) {
            LOG.info("getInputSplitForVertices: Reserved " + inputSplitPath +
                 " from ZooKeeper and got input split '" +
                 inputSplit.toString() + "'");
        }
        return inputSplit;
    }

    /**
     * Read vertices from input split.
     *
     * @param inputSplit Input split to process with vertex reader
     * @return List of vertices.
     * @throws IOException
     * @throws InterruptedException
     */
    private VertexEdgeCount readVerticesFromInputSplit(
            InputSplit inputSplit) throws IOException, InterruptedException {
        VertexInputFormat<I, V, E, M> vertexInputFormat =
            BspUtils.<I, V, E, M>createVertexInputFormat(getConfiguration());
        VertexReader<I, V, E, M> vertexReader =
            vertexInputFormat.createVertexReader(inputSplit, getContext());
        vertexReader.initialize(inputSplit, getContext());
        long vertexCount = 0;
        long edgeCount = 0;
        while (vertexReader.nextVertex()) {
            BasicVertex<I, V, E, M> readerVertex =
                vertexReader.getCurrentVertex();
            if (readerVertex.getVertexId() == null) {
                throw new IllegalArgumentException(
                    "loadVertices: Vertex reader returned a vertex " +
                    "without an id!  - " + readerVertex);
            }
            if (readerVertex.getVertexValue() == null) {
                readerVertex.setVertexValue(
                    BspUtils.<V>createVertexValue(getConfiguration()));
            }
            PartitionOwner partitionOwner =
                workerGraphPartitioner.getPartitionOwner(
                    readerVertex.getVertexId());
            Partition<I, V, E, M> partition =
                inputSplitCache.get(partitionOwner);
            if (partition == null) {
                partition = new Partition<I, V, E, M>(
                    getConfiguration(),
                    partitionOwner.getPartitionId());
                inputSplitCache.put(partitionOwner, partition);
            }
            partition.putVertex(readerVertex);
            if (partition.getVertices().size() >= maxVerticesPerPartition) {
                commService.sendPartitionReq(partitionOwner.getWorkerInfo(),
                                             partition);
                partition.getVertices().clear();
            }
            ++vertexCount;
            edgeCount += readerVertex.getNumOutEdges();
            getContext().progress();

            ++totalVerticesLoaded;
            totalEdgesLoaded += readerVertex.getNumOutEdges();
            // Update status every half a million vertices
            if ((totalVerticesLoaded % 500000) == 0) {
                String status = "readVerticesFromInputSplit: Loaded " +
                    totalVerticesLoaded + " vertices and " +
                    totalEdgesLoaded + " edges " +
                    ", totalMem = " + Runtime.getRuntime().totalMemory() +
                    " maxMem ="  + Runtime.getRuntime().maxMemory() +
                    " freeMem=" + Runtime.getRuntime().freeMemory() + " " +
                    getGraphMapper().getMapFunctions().toString() +
                    " - Attempt=" + getApplicationAttempt() +
                    ", Superstep=" + getSuperstep();
                if (LOG.isInfoEnabled()) {
                    LOG.info(status);
                }
                getContext().setStatus(status);
            }
        }
        vertexReader.close();

        return new VertexEdgeCount(vertexCount, edgeCount);
    }

    @Override
    public void setup() {
        // Unless doing a restart, prepare for computation:
        // 1. Start superstep INPUT_SUPERSTEP (no computation)
        // 2. Wait for the INPUT_SPLIT_READY_PATH node has been created
        // 3. Process input splits until there are no more.
        // 4. Wait for superstep INPUT_SUPERSTEP to complete.
        if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
            setCachedSuperstep(getRestartedSuperstep());
            return;
        }

        JSONObject jobState = getJobState();
        if (jobState != null) {
            try {
                if ((ApplicationState.valueOf(jobState.getString(JSONOBJ_STATE_KEY)) ==
                        ApplicationState.START_SUPERSTEP) &&
                        jobState.getLong(JSONOBJ_SUPERSTEP_KEY) ==
                        getSuperstep()) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("setup: Restarting from an automated " +
                                 "checkpointed superstep " +
                                 getSuperstep() + ", attempt " +
                                 getApplicationAttempt());
                    }
                    setRestartedSuperstep(getSuperstep());
                    return;
                }
            } catch (JSONException e) {
                throw new RuntimeException(
                    "setup: Failed to get key-values from " +
                    jobState.toString(), e);
            }
        }

        // Add the partitions for that this worker owns
        Collection<? extends PartitionOwner> masterSetPartitionOwners =
            startSuperstep();

        workerGraphPartitioner.updatePartitionOwners(
            getWorkerInfo(), masterSetPartitionOwners, getPartitionMap());
        commService.setup();

        // Ensure the InputSplits are ready for processing before processing
        while (true) {
            Stat inputSplitsReadyStat;
            try {
                inputSplitsReadyStat =
                    getZkExt().exists(INPUT_SPLITS_ALL_READY_PATH, true);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "setup: KeeperException waiting on input splits", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "setup: InterrupteException waiting on input splits", e);
            }
            if (inputSplitsReadyStat != null) {
                break;
            }
            getInputSplitsAllReadyEvent().waitForever();
            getInputSplitsAllReadyEvent().reset();
        }

        getContext().progress();

        try {
            VertexEdgeCount vertexEdgeCount = loadVertices();
            if (LOG.isInfoEnabled()) {
                LOG.info("setup: Finally loaded a total of " +
                         vertexEdgeCount);
            }
        } catch (Exception e) {
            LOG.error("setup: loadVertices failed - ", e);
            throw new IllegalStateException("setup: loadVertices failed", e);
        }
        getContext().progress();

        // At this point all vertices have been sent to their destinations.
        // Move them to the worker, creating creating the empty partitions
        movePartitionsToWorker(commService);
        for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
            if (partitionOwner.getWorkerInfo().equals(getWorkerInfo()) &&
                !getPartitionMap().containsKey(
                    partitionOwner.getPartitionId())) {
                Partition<I, V, E, M> partition =
                    new Partition<I, V, E, M>(getConfiguration(),
                                              partitionOwner.getPartitionId());
                getPartitionMap().put(partitionOwner.getPartitionId(),
                                      partition);
            }
        }

        // Generate the partition stats for the input superstep and process
        // if necessary
        List<PartitionStats> partitionStatsList =
            new ArrayList<PartitionStats>();
        for (Partition<I, V, E, M> partition : getPartitionMap().values()) {
            PartitionStats partitionStats =
                new PartitionStats(partition.getPartitionId(),
                                   partition.getVertices().size(),
                                   0,
                                   partition.getEdgeCount());
            partitionStatsList.add(partitionStats);
        }
        workerGraphPartitioner.finalizePartitionStats(
            partitionStatsList, workerPartitionMap);

        finishSuperstep(partitionStatsList);
    }

    /**
     *  Marshal the aggregator values of to a JSONArray that will later be
     *  aggregated by master.  Reset the 'use' of aggregators in the next
     *  superstep
     *
     * @param superstep
     */
    private JSONArray marshalAggregatorValues(long superstep) {
        JSONArray aggregatorArray = new JSONArray();
        if ((superstep == INPUT_SUPERSTEP) || (aggregatorInUse.size() == 0)) {
            return aggregatorArray;
        }

        for (String name : aggregatorInUse) {
            try {
                Aggregator<Writable> aggregator = getAggregatorMap().get(name);
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                aggregator.getAggregatedValue().write(output);

                JSONObject aggregatorObj = new JSONObject();
                aggregatorObj.put(AGGREGATOR_NAME_KEY, name);
                aggregatorObj.put(AGGREGATOR_CLASS_NAME_KEY,
                                  aggregator.getClass().getName());
                aggregatorObj.put(
                    AGGREGATOR_VALUE_KEY,
                    Base64.encodeBytes(outputStream.toByteArray()));
                aggregatorArray.put(aggregatorObj);
                LOG.info("marshalAggregatorValues: " +
                         "Found aggregatorObj " +
                         aggregatorObj + ", value (" +
                         aggregator.getAggregatedValue() + ")");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (LOG.isInfoEnabled()) {
	        LOG.info("marshalAggregatorValues: Finished assembling " +
	                 "aggregator values in JSONArray - " + aggregatorArray);
        }
        aggregatorInUse.clear();
        return aggregatorArray;
    }

    /**
     * Get values of aggregators aggregated by master in previous superstep.
     *
     * @param superstep Superstep to get the aggregated values from
     */
    private void getAggregatorValues(long superstep) {
        if (superstep <= (INPUT_SUPERSTEP + 1)) {
            return;
        }
        String mergedAggregatorPath =
            getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
        JSONArray aggregatorArray = null;
        try {
            byte[] zkData =
                getZkExt().getData(mergedAggregatorPath, false, null);
            aggregatorArray = new JSONArray(new String(zkData));
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getAggregatorValues: no aggregators in " +
                     mergedAggregatorPath + " on superstep " + superstep);
            return;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < aggregatorArray.length(); ++i) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getAggregatorValues: " +
                              "Getting aggregators from " +
                              aggregatorArray.getJSONObject(i));
                }
                String aggregatorName = aggregatorArray.getJSONObject(i).
                    getString(AGGREGATOR_NAME_KEY);
                Aggregator<Writable> aggregator =
                    getAggregatorMap().get(aggregatorName);
                if (aggregator == null) {
                    continue;
                }
                Writable aggregatorValue = aggregator.getAggregatedValue();
                InputStream input =
                    new ByteArrayInputStream(
                        Base64.decode(aggregatorArray.getJSONObject(i).
                            getString(AGGREGATOR_VALUE_KEY)));
                aggregatorValue.readFields(
                    new DataInputStream(input));
                aggregator.setAggregatedValue(aggregatorValue);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getAggregatorValues: " +
                              "Got aggregator=" + aggregatorName + " value=" +
                               aggregatorValue);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("getAggregatorValues: Finished loading " +
                     mergedAggregatorPath + " with aggregator values " +
                     aggregatorArray);
        }
    }

    /**
     * Register the health of this worker for a given superstep
     *
     * @param superstep Superstep to register health on
     */
    private void registerHealth(long superstep) {
        JSONArray hostnamePort = new JSONArray();
        hostnamePort.put(getHostname());

        hostnamePort.put(workerInfo.getPort());

        String myHealthPath = null;
        if (isHealthy()) {
            myHealthPath = getWorkerInfoHealthyPath(getApplicationAttempt(),
                                                    getSuperstep());
        }
        else {
            myHealthPath = getWorkerInfoUnhealthyPath(getApplicationAttempt(),
                                                      getSuperstep());
        }
        myHealthPath = myHealthPath + "/" + workerInfo.getHostnameId();
        try {
            myHealthZnode = getZkExt().createExt(
                myHealthPath,
                WritableUtils.writeToByteArray(workerInfo),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("registerHealth: myHealthPath already exists (likely " +
                     "from previous failure): " + myHealthPath +
                     ".  Waiting for change in attempts " +
                     "to re-join the application");
            getApplicationAttemptChangedEvent().waitForever();
            if (LOG.isInfoEnabled()) {
                LOG.info("registerHealth: Got application " +
                         "attempt changed event, killing self");
            }
            throw new RuntimeException(
                "registerHealth: Trying " +
                "to get the new application attempt by killing self", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("registerHealth: Created my health node for attempt=" +
                     getApplicationAttempt() + ", superstep=" +
                     getSuperstep() + " with " + myHealthZnode +
                     " and workerInfo= " + workerInfo);
        }
    }

    @Override
    public Collection<? extends PartitionOwner> startSuperstep() {
        // Algorithm:
        // 1. Communication service will combine message from previous
        //    superstep
        // 2. Register my health for the next superstep.
        // 3. Wait until the partition assignment is complete and get it
        // 4. Get the aggregator values from the previous superstep
        if (getSuperstep() != INPUT_SUPERSTEP) {
            commService.prepareSuperstep();
        }

        registerHealth(getSuperstep());

        String partitionAssignmentsNode =
            getPartitionAssignmentsPath(getApplicationAttempt(),
                                        getSuperstep());
        Collection<? extends PartitionOwner> masterSetPartitionOwners;
        try {
            while (getZkExt().exists(partitionAssignmentsNode, true) ==
                    null) {
                getPartitionAssignmentsReadyChangedEvent().waitForever();
                getPartitionAssignmentsReadyChangedEvent().reset();
            }
            List<? extends Writable> writableList =
                WritableUtils.readListFieldsFromZnode(
                    getZkExt(),
                    partitionAssignmentsNode,
                    false,
                    null,
                    workerGraphPartitioner.createPartitionOwner().getClass(),
                    getConfiguration());

            @SuppressWarnings("unchecked")
            Collection<? extends PartitionOwner> castedWritableList =
                (Collection<? extends PartitionOwner>) writableList;
            masterSetPartitionOwners = castedWritableList;
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "startSuperstep: KeeperException getting assignments", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "startSuperstep: InterruptedException getting assignments", e);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("startSuperstep: Ready for computation on superstep " +
                     getSuperstep() + " since worker " +
                     "selection and vertex range assignments are done in " +
                     partitionAssignmentsNode);
        }

        if (getSuperstep() != INPUT_SUPERSTEP) {
            getAggregatorValues(getSuperstep());
        }
        getContext().setStatus("startSuperstep: " +
                               getGraphMapper().getMapFunctions().toString() +
                               " - Attempt=" + getApplicationAttempt() +
                               ", Superstep=" + getSuperstep());
        return masterSetPartitionOwners;
    }

    @Override
    public boolean finishSuperstep(List<PartitionStats> partitionStatsList) {
        // This barrier blocks until success (or the master signals it to
        // restart).
        //
        // Master will coordinate the barriers and aggregate "doneness" of all
        // the vertices.  Each worker will:
        // 1. Save aggregator values that are in use.
        // 2. Report the statistics (vertices, edges, messages, etc.)
        // of this worker
        // 3. Let the master know it is finished.
        // 4. Then it waits for the master to say whether to stop or not.
        long workerSentMessages = 0;
        try {
            workerSentMessages = commService.flush(getContext());
        } catch (IOException e) {
            throw new IllegalStateException(
                "finishSuperstep: flush failed", e);
        }
        JSONArray aggregatorValueArray =
            marshalAggregatorValues(getSuperstep());
        Collection<PartitionStats> finalizedPartitionStats =
            workerGraphPartitioner.finalizePartitionStats(
                partitionStatsList, workerPartitionMap);
        List<PartitionStats> finalizedPartitionStatsList =
            new ArrayList<PartitionStats>(finalizedPartitionStats);
        byte [] partitionStatsBytes =
            WritableUtils.writeListToByteArray(finalizedPartitionStatsList);
        JSONObject workerFinishedInfoObj = new JSONObject();
        try {
            workerFinishedInfoObj.put(JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY,
                                      aggregatorValueArray);
            workerFinishedInfoObj.put(JSONOBJ_PARTITION_STATS_KEY,
                                      Base64.encodeBytes(partitionStatsBytes));
            workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGES_KEY,
                                      workerSentMessages);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        String finishedWorkerPath =
            getWorkerFinishedPath(getApplicationAttempt(), getSuperstep()) +
            "/" + getHostnamePartitionId();
        try {
            getZkExt().createExt(finishedWorkerPath,
                                 workerFinishedInfoObj.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("finishSuperstep: finished worker path " +
                     finishedWorkerPath + " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        getContext().setStatus("finishSuperstep: (waiting for rest " +
                               "of workers) " +
                               getGraphMapper().getMapFunctions().toString() +
                               " - Attempt=" + getApplicationAttempt() +
                               ", Superstep=" + getSuperstep());

        String superstepFinishedNode =
            getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
        try {
            while (getZkExt().exists(superstepFinishedNode, true) == null) {
                getSuperstepFinishedEvent().waitForever();
                getSuperstepFinishedEvent().reset();
            }
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "finishSuperstep: Failed while waiting for master to " +
                "signal completion of superstep " + getSuperstep(), e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "finishSuperstep: Failed while waiting for master to " +
                "signal completion of superstep " + getSuperstep(), e);
        }
        GlobalStats globalStats = new GlobalStats();
        WritableUtils.readFieldsFromZnode(
            getZkExt(), superstepFinishedNode, false, null, globalStats);
        if (LOG.isInfoEnabled()) {
            LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
                     " with global stats " + globalStats);
        }
        incrCachedSuperstep();
        getContext().setStatus("finishSuperstep: (all workers done) " +
                               getGraphMapper().getMapFunctions().toString() +
                               " - Attempt=" + getApplicationAttempt() +
                               ", Superstep=" + getSuperstep());
        getGraphMapper().getGraphState().
            setNumEdges(globalStats.getEdgeCount()).
            setNumVertices(globalStats.getVertexCount());
        return ((globalStats.getFinishedVertexCount() ==
                globalStats.getVertexCount()) &&
                (globalStats.getMessageCount() == 0));
    }

    /**
     * Save the vertices using the user-defined VertexOutputFormat from our
     * vertexArray based on the split.
     * @throws InterruptedException
     */
    private void saveVertices() throws IOException, InterruptedException {
        if (getConfiguration().get(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS)
                == null) {
            LOG.warn("saveVertices: " + GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS +
                     " not specified -- there will be no saved output");
            return;
        }

        VertexOutputFormat<I, V, E> vertexOutputFormat =
            BspUtils.<I, V, E>createVertexOutputFormat(getConfiguration());
        VertexWriter<I, V, E> vertexWriter =
            vertexOutputFormat.createVertexWriter(getContext());
        vertexWriter.initialize(getContext());
        for (Partition<I, V, E, M> partition : workerPartitionMap.values()) {
            for (BasicVertex<I, V, E, M> vertex : partition.getVertices()) {
                vertexWriter.writeVertex(vertex);
            }
        }
        vertexWriter.close(getContext());
    }

    @Override
    public void cleanup() throws IOException, InterruptedException {
        commService.closeConnections();
        setCachedSuperstep(getSuperstep() - 1);
        saveVertices();
         // All worker processes should denote they are done by adding special
         // znode.  Once the number of znodes equals the number of partitions
         // for workers and masters, the master will clean up the ZooKeeper
         // znodes associated with this job.
        String cleanedUpPath = CLEANED_UP_PATH  + "/" +
            getTaskPartition() + WORKER_SUFFIX;
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
            // Cleaning up, it's okay to fail after cleanup is successful
            LOG.error("cleanup: Got KeeperException on notifcation " +
                      "to master about cleanup", e);
        } catch (InterruptedException e) {
            // Cleaning up, it's okay to fail after cleanup is successful
            LOG.error("cleanup: Got InterruptedException on notifcation " +
                      "to master about cleanup", e);
        }
        try {
            getZkExt().close();
        } catch (InterruptedException e) {
            // cleanup phase -- just log the error
            LOG.error("cleanup: Zookeeper failed to close with " + e);
        }

        // Preferably would shut down the service only after
        // all clients have disconnected (or the exceptions on the
        // client side ignored).
        commService.close();
    }

    @Override
    public void storeCheckpoint() throws IOException {
        getContext().setStatus("storeCheckpoint: Starting checkpoint " +
                getGraphMapper().getMapFunctions().toString() +
                " - Attempt=" + getApplicationAttempt() +
                ", Superstep=" + getSuperstep());

        // Algorithm:
        // For each partition, dump vertices and messages
        Path metadataFilePath =
            new Path(getCheckpointBasePath(getSuperstep()) + "." +
                     getHostnamePartitionId() +
                     CHECKPOINT_METADATA_POSTFIX);
        Path verticesFilePath =
            new Path(getCheckpointBasePath(getSuperstep()) + "." +
                     getHostnamePartitionId() +
                     CHECKPOINT_VERTICES_POSTFIX);
        Path validFilePath =
            new Path(getCheckpointBasePath(getSuperstep()) + "." +
                     getHostnamePartitionId() +
                     CHECKPOINT_VALID_POSTFIX);

        // Remove these files if they already exist (shouldn't though, unless
        // of previous failure of this worker)
        if (getFs().delete(validFilePath, false)) {
            LOG.warn("storeCheckpoint: Removed valid file " +
                     validFilePath);
        }
        if (getFs().delete(metadataFilePath, false)) {
            LOG.warn("storeCheckpoint: Removed metadata file " +
                     metadataFilePath);
        }
        if (getFs().delete(verticesFilePath, false)) {
            LOG.warn("storeCheckpoint: Removed file " + verticesFilePath);
        }

        FSDataOutputStream verticesOutputStream =
            getFs().create(verticesFilePath);
        ByteArrayOutputStream metadataByteStream = new ByteArrayOutputStream();
        DataOutput metadataOutput = new DataOutputStream(metadataByteStream);
        for (Partition<I, V, E, M> partition : workerPartitionMap.values()) {
            long startPos = verticesOutputStream.getPos();
            partition.write(verticesOutputStream);
            // Write the metadata for this partition
            // Format:
            // <index count>
            //   <index 0 start pos><partition id>
            //   <index 1 start pos><partition id>
            metadataOutput.writeLong(startPos);
            metadataOutput.writeInt(partition.getPartitionId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("storeCheckpoint: Vertex file starting " +
                          "offset = " + startPos + ", length = " +
                          (verticesOutputStream.getPos() - startPos) +
                          ", partition = " + partition.toString());
            }
        }
        // Metadata is buffered and written at the end since it's small and
        // needs to know how many partitions this worker owns
        FSDataOutputStream metadataOutputStream =
            getFs().create(metadataFilePath);
        metadataOutputStream.writeInt(workerPartitionMap.size());
        metadataOutputStream.write(metadataByteStream.toByteArray());
        metadataOutputStream.close();
        verticesOutputStream.close();
        if (LOG.isInfoEnabled()) {
            LOG.info("storeCheckpoint: Finished metadata (" +
                     metadataFilePath + ") and vertices (" + verticesFilePath
                     + ").");
        }

        getFs().createNewFile(validFilePath);
    }

    @Override
    public void loadCheckpoint(long superstep) {
        // Algorithm:
        // Examine all the partition owners and load the ones
        // that match my hostname and id from the master designated checkpoint
        // prefixes.
        long startPos = 0;
        int loadedPartitions = 0;
        for (PartitionOwner partitionOwner :
                workerGraphPartitioner.getPartitionOwners()) {
            if (partitionOwner.getWorkerInfo().equals(getWorkerInfo())) {
                String metadataFile =
                    partitionOwner.getCheckpointFilesPrefix() +
                    CHECKPOINT_METADATA_POSTFIX;
                String partitionsFile =
                    partitionOwner.getCheckpointFilesPrefix() +
                    CHECKPOINT_VERTICES_POSTFIX;
                try {
                    int partitionId = -1;
                    DataInputStream metadataStream =
                        getFs().open(new Path(metadataFile));
                    int partitions = metadataStream.readInt();
                    for (int i = 0; i < partitions; ++i) {
                        startPos = metadataStream.readLong();
                        partitionId = metadataStream.readInt();
                        if (partitionId == partitionOwner.getPartitionId()) {
                            break;
                        }
                    }
                    if (partitionId != partitionOwner.getPartitionId()) {
                        throw new IllegalStateException(
                           "loadCheckpoint: " + partitionOwner +
                           " not found!");
                    }
                    metadataStream.close();
                    Partition<I, V, E, M> partition =
                        new Partition<I, V, E, M>(
                            getConfiguration(),
                            partitionId);
                    DataInputStream partitionsStream =
                        getFs().open(new Path(partitionsFile));
                    if (partitionsStream.skip(startPos) != startPos) {
                        throw new IllegalStateException(
                            "loadCheckpoint: Failed to skip " + startPos +
                            " on " + partitionsFile);
                    }
                    partition.readFields(partitionsStream);
                    partitionsStream.close();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("loadCheckpoint: Loaded partition " +
                                 partition);
                    }
                    if (getPartitionMap().put(partitionId, partition) != null) {
                        throw new IllegalStateException(
                            "loadCheckpoint: Already has partition owner " +
                            partitionOwner);
                    }
                    ++loadedPartitions;
                } catch (IOException e) {
                    throw new RuntimeException(
                        "loadCheckpoing: Failed to get partition owner " +
                        partitionOwner, e);
                }
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("loadCheckpoint: Loaded " + loadedPartitions +
                    " partitions of out " +
                    workerGraphPartitioner.getPartitionOwners().size() +
                    " total.");
        }
        // Communication service needs to setup the connections prior to
        // processing vertices
        commService.setup();
    }

    /**
     * Send the worker partitions to their destination workers
     *
     * @param workerPartitionMap Map of worker info to the partitions stored
     *        on this worker to be sent
     */
    private void sendWorkerPartitions(
            Map<WorkerInfo, List<Integer>> workerPartitionMap) {
        List<Entry<WorkerInfo, List<Integer>>> randomEntryList =
            new ArrayList<Entry<WorkerInfo, List<Integer>>>(
                workerPartitionMap.entrySet());
        Collections.shuffle(randomEntryList);
        for (Entry<WorkerInfo, List<Integer>> workerPartitionList :
                randomEntryList) {
            for (Integer partitionId : workerPartitionList.getValue()) {
                Partition<I, V, E, M> partition =
                    getPartitionMap().get(partitionId);
                if (partition == null) {
                    throw new IllegalStateException(
                        "sendWorkerPartitions: Couldn't find partition " +
                        partitionId + " to send to " +
                        workerPartitionList.getKey());
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("sendWorkerPartitions: Sending worker " +
                             workerPartitionList.getKey() + " partition " +
                             partitionId);
                }
                getGraphMapper().getGraphState().getWorkerCommunications().
                sendPartitionReq(workerPartitionList.getKey(),
                                 partition);
                getPartitionMap().remove(partitionId);
            }
        }

        String myPartitionExchangeDonePath =
            getPartitionExchangeWorkerPath(
                getApplicationAttempt(), getSuperstep(), getWorkerInfo());
        try {
            getZkExt().createExt(myPartitionExchangeDonePath,
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                    "sendWorkerPartitions: KeeperException to create " +
                    myPartitionExchangeDonePath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                    "sendWorkerPartitions: InterruptedException to create " +
                    myPartitionExchangeDonePath, e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("sendWorkerPartitions: Done sending all my partitions.");
        }
    }

    @Override
    public final void exchangeVertexPartitions(
            Collection<? extends PartitionOwner> masterSetPartitionOwners) {
        // 1. Fix the addresses of the partition ids if they have changed.
        // 2. Send all the partitions to their destination workers in a random
        //    fashion.
        // 3. Notify completion with a ZooKeeper stamp
        // 4. Wait for all my dependencies to be done (if any)
        // 5. Add the partitions to myself.
        PartitionExchange partitionExchange =
            workerGraphPartitioner.updatePartitionOwners(
                getWorkerInfo(), masterSetPartitionOwners, getPartitionMap());
        commService.fixPartitionIdToSocketAddrMap();

        Map<WorkerInfo, List<Integer>> workerPartitionMap =
            partitionExchange.getSendWorkerPartitionMap();
        if (!workerPartitionMap.isEmpty()) {
            sendWorkerPartitions(workerPartitionMap);
        }

        Set<WorkerInfo> myDependencyWorkerSet =
            partitionExchange.getMyDependencyWorkerSet();
        Set<String> workerIdSet = new HashSet<String>();
        for (WorkerInfo workerInfo : myDependencyWorkerSet) {
            if (workerIdSet.add(workerInfo.getHostnameId()) != true) {
                throw new IllegalStateException(
                    "exchangeVertexPartitions: Duplicate entry " + workerInfo);
            }
        }
        if (myDependencyWorkerSet.isEmpty() && workerPartitionMap.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("exchangeVertexPartitions: Nothing to exchange, " +
                         "exiting early");
            }
            return;
        }

        String vertexExchangePath =
            getPartitionExchangePath(getApplicationAttempt(), getSuperstep());
        List<String> workerDoneList;
        try {
            while (true) {
                workerDoneList = getZkExt().getChildrenExt(
                    vertexExchangePath, true, false, false);
                workerIdSet.removeAll(workerDoneList);
                if (workerIdSet.isEmpty()) {
                    break;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("exchangeVertexPartitions: Waiting for workers " +
                             workerIdSet);
                }
                getPartitionExchangeChildrenChangedEvent().waitForever();
                getPartitionExchangeChildrenChangedEvent().reset();
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("exchangeVertexPartitions: Done with exchange.");
        }

        // Add the partitions sent earlier
        movePartitionsToWorker(commService);
    }

    /**
     * Partitions that are exchanged need to be moved from the communication
     * service to the worker.
     *
     * @param commService Communication service where the partitions are
     *        temporarily stored.
     */
    private void movePartitionsToWorker(
            ServerInterface<I, V, E, M> commService) {
        Map<Integer, List<BasicVertex<I, V, E, M>>> inPartitionVertexMap =
                commService.getInPartitionVertexMap();
        synchronized (inPartitionVertexMap) {
            for (Entry<Integer, List<BasicVertex<I, V, E, M>>> entry :
                inPartitionVertexMap.entrySet()) {
                if (getPartitionMap().containsKey(entry.getKey())) {
                    throw new IllegalStateException(
                        "moveVerticesToWorker: Already has partition " +
                         entry.getKey());
                }

                Partition<I, V, E, M> tmpPartition = new Partition<I, V, E, M>(
                    getConfiguration(),
                    entry.getKey());
                for (BasicVertex<I, V, E, M> vertex : entry.getValue()) {
                    if (tmpPartition.putVertex(vertex) != null) {
                        throw new IllegalStateException(
                            "moveVerticesToWorker: Vertex " + vertex +
                            " already exists!");
                    }
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("moveVerticesToWorker: Adding " +
                            entry.getValue().size() +
                            " vertices for partition id " + entry.getKey());
                }
                getPartitionMap().put(tmpPartition.getPartitionId(),
                                      tmpPartition);
                entry.getValue().clear();
            }
            inPartitionVertexMap.clear();
        }
    }

    final public BspEvent getPartitionExchangeChildrenChangedEvent() {
        return partitionExchangeChildrenChanged;
    }

    @Override
    protected boolean processEvent(WatchedEvent event) {
        boolean foundEvent = false;
        if (event.getPath().startsWith(MASTER_JOB_STATE_PATH) &&
                (event.getType() == EventType.NodeChildrenChanged)) {
            if (LOG.isInfoEnabled()) {
                LOG.info("processEvent: Job state changed, checking " +
                         "to see if it needs to restart");
            }
            JSONObject jsonObj = getJobState();
            try {
                if ((ApplicationState.valueOf(jsonObj.getString(JSONOBJ_STATE_KEY)) ==
                        ApplicationState.START_SUPERSTEP) &&
                        jsonObj.getLong(JSONOBJ_APPLICATION_ATTEMPT_KEY) !=
                        getApplicationAttempt()) {
                    LOG.fatal("processEvent: Worker will restart " +
                              "from command - " + jsonObj.toString());
                    System.exit(-1);
                }
            } catch (JSONException e) {
                throw new RuntimeException(
                    "processEvent: Couldn't properly get job state from " +
                    jsonObj.toString());
            }
            foundEvent = true;
        } else if (event.getPath().contains(PARTITION_EXCHANGE_DIR) &&
                   event.getType() == EventType.NodeChildrenChanged) {
            if (LOG.isInfoEnabled()) {
                LOG.info("processEvent : partitionExchangeChildrenChanged " +
                         "(at least one worker is done sending partitions)");
            }
            partitionExchangeChildrenChanged.signal();
            foundEvent = true;
        }

        return foundEvent;
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    @Override
    public Map<Integer, Partition<I, V, E, M>> getPartitionMap() {
        return workerPartitionMap;
    }

    @Override
    public Collection<? extends PartitionOwner> getPartitionOwners() {
        return workerGraphPartitioner.getPartitionOwners();
    }

    @Override
    public PartitionOwner getVertexPartitionOwner(I vertexIndex) {
        return workerGraphPartitioner.getPartitionOwner(vertexIndex);
    }

    public Partition<I, V, E, M> getPartition(I vertexIndex) {
        PartitionOwner partitionOwner = getVertexPartitionOwner(vertexIndex);
        return workerPartitionMap.get(partitionOwner.getPartitionId());
    }

    @Override
    public BasicVertex<I, V, E, M> getVertex(I vertexIndex) {
        PartitionOwner partitionOwner = getVertexPartitionOwner(vertexIndex);
        if (workerPartitionMap.containsKey(partitionOwner.getPartitionId())) {
            return workerPartitionMap.get(
                partitionOwner.getPartitionId()).getVertex(vertexIndex);
        } else {
            return null;
        }
    }
}
