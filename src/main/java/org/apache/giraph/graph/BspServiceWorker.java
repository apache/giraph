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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

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
    /** Cached aggregate number of vertices in the entire application */
    private long totalVertices = -1;
    /** Cached aggregate number of edges in the entire application */
    private long totalEdges = -1;
    /** My process health znode */
    private String myHealthZnode;
    /** Final server RPC port */
    private final int finalRpcPort;
    /** List of aggregators currently in use */
    private Set<String> aggregatorInUse = new TreeSet<String>();
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);

    public BspServiceWorker(String serverPortList,
                            int sessionMsecTimeout,
                            Mapper<?, ?, ?, ?>.Context context,
                            GiraphJob.BspMapper<I, V, E, M> bspMapper) {
        super(serverPortList, sessionMsecTimeout, context, bspMapper);
        this.finalRpcPort =
            getConfiguration().getInt(GiraphJob.RPC_INITIAL_PORT,
                          GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
                          getTaskPartition();
    }

    public int getPort() {
        return finalRpcPort;
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
                        LOG.info("reserveInputSplit: Reserved input split " +
                                 "path " + reservedInputSplitPath);
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
     * Each worker will set the vertex ranges that it has found for a given
     * InputSplit. After this, the InputSplit is considered finished.
     *
     * @param inputSplitPath path to the input split znode
     * @param maxIndexStatMap maps max vertex indexes to a list containing
     *        the number of vertices (index 0) and the number of edges (index 1)
     *        in each partition (can be null, where nothing is written)
     */
    private void setInputSplitVertexRanges(
        String inputSplitPath,
        Map<I, List<Long>> maxIndexStatMap) {
        String inputSplitFinishedPath =
            inputSplitPath + INPUT_SPLIT_FINISHED_NODE;
        byte [] zkData = null;
        JSONArray statArray = new JSONArray();
        if (maxIndexStatMap != null) {
            for (Map.Entry<I, List<Long>> entry : maxIndexStatMap.entrySet()) {
                try {
                    ByteArrayOutputStream outputStream =
                        new ByteArrayOutputStream();
                    DataOutput output = new DataOutputStream(outputStream);
                    ((Writable) entry.getKey()).write(output);

                    JSONObject vertexRangeObj = new JSONObject();
                    vertexRangeObj.put(JSONOBJ_NUM_VERTICES_KEY,
                                       entry.getValue().get(0));
                    vertexRangeObj.put(JSONOBJ_NUM_EDGES_KEY,
                                       entry.getValue().get(1));
                    vertexRangeObj.put(JSONOBJ_HOSTNAME_ID_KEY,
                                       getHostnamePartitionId());
                    vertexRangeObj.put(JSONOBJ_MAX_VERTEX_INDEX_KEY,
                                       outputStream.toString("UTF-8"));
                    statArray.put(vertexRangeObj);
                    LOG.info("setInputSplitVertexRanges: " +
                             "Trying to add vertexRangeObj " +
                             vertexRangeObj + " to InputSplit path " +
                             inputSplitPath);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            zkData = statArray.toString().getBytes();
        }
        try {
            getZkExt().createExt(inputSplitFinishedPath,
                                 zkData,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("setLocalVertexRanges: " + inputSplitFinishedPath +
                     " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("setInputSplitVertexRanges: Finished loading " +
                 inputSplitPath + " with vertexRanges - " + statArray);
    }

    /**
     * Load the vertices from the user-defined VertexReader into our partitions
     * of vertex ranges.  Do this until all the InputSplits have been processed.
     * All workers will try to do as many InputSplits as they can.  The master
     * will monitor progress and stop this once all the InputSplits have been
     * loaded and check-pointed.  The InputSplits must be sorted.
     *
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    private void loadVertices() throws
            InstantiationException, IllegalAccessException,
            IOException, ClassNotFoundException {
        List<Vertex<I, V, E, M>> vertexList =
            new ArrayList<Vertex<I, V, E, M>>();
        String inputSplitPath = null;
        while ((inputSplitPath = reserveInputSplit()) != null) {
            // ZooKeeper has a limit of the data in a single znode of 1 MB and
            // each entry can go be on the average somewhat more than 300 bytes
            final long maxVertexRangesPerInputSplit =
                1024 * 1024 / 350 / inputSplitCount;

            byte[] splitList;
            try {
                splitList = getZkExt().getData(inputSplitPath, false, null);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "loadVertices: KeeperException on " + inputSplitPath, e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "loadVertices: IllegalStateException on " +
                    inputSplitPath, e);
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
                LOG.info("loadVertices: Reserved " + inputSplitPath +
                         " from ZooKeeper and got input split '" +
                         inputSplit.toString() + "'");
            }
            VertexInputFormat<I, V, E> vertexInputFormat =
                BspUtils.<I, V, E>createVertexInputFormat(getConfiguration());
            VertexReader<I, V, E> vertexReader =
                vertexInputFormat.createVertexReader(inputSplit, getContext());
            vertexReader.initialize(inputSplit, getContext());
            vertexList.clear();
            try {
                Vertex<I, V, E, M> vertex =
                    BspUtils.<I, V, E, M>createVertex(getConfiguration());
                while (vertexReader.next(vertex)) {
                    if (vertex.getVertexValue() == null) {
                        vertex.setVertexValue(
                            BspUtils.<V>createVertexValue(getConfiguration()));
                    }
                    vertexList.add(vertex);
                    vertex =
                        BspUtils.<I, V, E, M>createVertex(getConfiguration());
                    getContext().progress();
                }
                vertexReader.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (vertexList.isEmpty()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("loadVertices: No vertices in input split " +
                             inputSplit);
                }
                // TODO: Need to add checkpoints
                setInputSplitVertexRanges(inputSplitPath, null);
                continue;
            }

            // Separate all the vertices in this InputSplit into vertex ranges.
            // The number of vertex ranges is up to half of the number of
            // available workers and must reach a minimum size.  Then two passes
            // over the vertexList.  First, find the maximum vertex ranges.
            // Then fill them in.
            NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap =
                new TreeMap<I, VertexRange<I, V, E, M>>();
            long vertexRangesPerInputSplit = (long) (inputSplitCount *
                getConfiguration().getFloat(
                    GiraphJob.TOTAL_INPUT_SPLIT_MULTIPLIER,
                    GiraphJob.TOTAL_INPUT_SPLIT_MULTIPLIER_DEFAULT));
            if (vertexRangesPerInputSplit == 0) {
                vertexRangesPerInputSplit = 1;
            }
            else if (vertexRangesPerInputSplit > maxVertexRangesPerInputSplit) {
                LOG.warn("loadVertices: Using " + maxVertexRangesPerInputSplit +
                         " instead of " + vertexRangesPerInputSplit +
                         " vertex ranges on input split " + inputSplit);
                vertexRangesPerInputSplit = maxVertexRangesPerInputSplit;
            }

            long vertexRangeSize =
                vertexList.size() / vertexRangesPerInputSplit;
            long minPerVertexRange =
                getConfiguration().getLong(
                    GiraphJob.MIN_VERTICES_PER_RANGE,
                    GiraphJob.MIN_VERTICES_PER_RANGE_DEFAULT);
            if (vertexRangeSize < minPerVertexRange) {
                vertexRangeSize = minPerVertexRange;
            }
            I vertexIdMax = null;
            for (int i = 0; i < vertexList.size(); ++i) {
                if ((vertexIdMax != null) && ((i % vertexRangeSize) == 0)) {
                    VertexRange<I, V, E, M> vertexRange =
                        new VertexRange<I, V, E, M>(
                            null, -1, null, vertexIdMax, null);
                    vertexRangeMap.put(vertexIdMax, vertexRange);
                    vertexIdMax = null;
                }

                if (vertexIdMax == null) {
                    vertexIdMax = vertexList.get(i).getVertexId();
                } else {
                    @SuppressWarnings("unchecked")
                    int compareTo =
                        vertexList.get(i).getVertexId().compareTo(vertexIdMax);
                    if (compareTo > 0) {
                        vertexIdMax = vertexList.get(i).getVertexId();
                    }
                }
            }
            if (vertexIdMax == null) {
                throw new RuntimeException("loadVertices: Encountered " +
                                           "impossible null vertexIdMax.");
            }
            VertexRange<I, V, E, M> vertexRange =
                new VertexRange<I, V, E, M>(
                    null, -1, null, vertexIdMax, null);
            vertexRangeMap.put(vertexIdMax, vertexRange);

            Iterator<I> maxIndexVertexMapIt =
                vertexRangeMap.keySet().iterator();
            I currentVertexIndexMax = maxIndexVertexMapIt.next();
            for (Vertex<I, V, E, M> vertex : vertexList) {
                @SuppressWarnings("unchecked")
                int compareTo =
                    vertex.getVertexId().compareTo(
                        currentVertexIndexMax);
                if (compareTo > 0) {
                    if (!maxIndexVertexMapIt.hasNext()) {
                        throw new RuntimeException(
                            "loadVertices: Impossible that vertex " +
                            vertex.getVertexId() + " > " +
                            currentVertexIndexMax);
                    }
                    currentVertexIndexMax = maxIndexVertexMapIt.next();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("loadVertices: Adding vertex with index = " +
                              vertex.getVertexId() + " to vertex range max = " +
                              currentVertexIndexMax);
                }
                if (vertexRangeMap.get(currentVertexIndexMax).
                        getVertexMap().put(vertex.getVertexId(),
                                           vertex) != null) {
                    throw new IllegalStateException(
                        "loadVertices: Already contains vertex " +
                        vertex.toString());
                }
            }
            Map<I, List<Long>> maxIndexStatMap = new TreeMap<I, List<Long>>();
            for (Entry<I, VertexRange<I, V, E, M>> entry :
                    vertexRangeMap.entrySet()) {
                List<Long> statList = new ArrayList<Long>();
                long vertexRangeEdgeCount = 0;
                for (BasicVertex<I, V, E, M> vertex :
                        entry.getValue().getVertexMap().values()) {
                    vertexRangeEdgeCount += vertex.getOutEdgeMap().size();
                }
                statList.add(Long.valueOf
                                 (entry.getValue().getVertexMap().size()));
                statList.add(Long.valueOf(vertexRangeEdgeCount));
                if (LOG.isInfoEnabled()) {
                    LOG.info("loadVertices: Got " + statList.get(0) +
                             " vertices and " + statList.get(1) +
                             " edges from vertex range max index " +
                             entry.getKey());
                }
                maxIndexStatMap.put(entry.getKey(), statList);

                // Add the local vertex ranges to the stored vertex ranges
                getVertexRangeMap().put(entry.getKey(), entry.getValue());
            }
            setInputSplitVertexRanges(inputSplitPath, maxIndexStatMap);
        }
    }

    @Override
    public void setup() {
        // Unless doing a restart, prepare for computation:
        // 1. Start superstep 0 (no computation)
        // 2. Wait for the INPUT_SPLIT_READY_PATH node has been created
        // 3. Process input splits until there are no more.
        // 4. Wait for superstep 0 to complete.
        if (getRestartedSuperstep() < -1) {
            throw new IllegalArgumentException(
                "setup: Invalid superstep to restart - " +
                getRestartedSuperstep());
        }
        else if (getRestartedSuperstep() > 0) {
            setCachedSuperstep(getRestartedSuperstep());
            return;
        }

        JSONObject jobState = getJobState();
        if (jobState != null) {
            try {
                if ((State.valueOf(jobState.getString(JSONOBJ_STATE_KEY)) ==
                        State.START_SUPERSTEP) &&
                        jobState.getLong(JSONOBJ_SUPERSTEP_KEY) ==
                            getSuperstep()) {
                    LOG.info("setup: Restarting from an automated " +
                             "checkpointed superstep " +
                             getSuperstep() + ", attempt " +
                             getApplicationAttempt());
                    setRestartedSuperstep(getSuperstep());
                    return;
                }
            } catch (JSONException e) {
                throw new RuntimeException(
                    "setup: Failed to get key-values from " +
                    jobState.toString(), e);
            }
        }

        startSuperstep();

        // Ensure the InputSplits are ready for processing before processing
        while (true) {
            Stat inputSplitsReadyStat;
            try {
                inputSplitsReadyStat =
                    getZkExt().exists(INPUT_SPLITS_ALL_READY_PATH, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (inputSplitsReadyStat != null) {
                break;
            }
            getInputSplitsAllReadyEvent().waitForever();
            getInputSplitsAllReadyEvent().reset();
        }

        getContext().progress();
        try {
            loadVertices();
        } catch (Exception e) {
            LOG.error("setup: loadVertices failed - " + e.getMessage());
            throw new RuntimeException(e);
        }

        Map<I, long []> maxIndexStatsMap = new TreeMap<I, long []>();
        for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
             getVertexRangeMap().entrySet()) {
            long [] statArray = new long[3];
            statArray[0] = 0;
            statArray[1] = entry.getValue().getVertexMap().size();
            statArray[2] = entry.getValue().getEdgeCount();
            maxIndexStatsMap.put(entry.getKey(), statArray);
        }

        finishSuperstep(maxIndexStatsMap);
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
        if (superstep == 0 || aggregatorInUse.size() == 0) {
            return aggregatorArray;
        }

        Base64 base64 = new Base64();
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
                    base64.encodeToString(outputStream.toByteArray()));
                aggregatorArray.put(aggregatorObj);
                LOG.info("marshalAggregatorValues: " +
                         "Found aggregatorObj " +
                         aggregatorObj + ", value (" +
                         aggregator.getAggregatedValue() + ")");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        LOG.info("marshalAggregatorValues: Finished assembling " +
                 "aggregator values in JSONArray - " + aggregatorArray);
        aggregatorInUse.clear();
        return aggregatorArray;
    }

    /**
     * Get values of aggregators aggregated by master in previous superstep.
     *
     * @param superstep
     */
    private void getAggregatorValues(long superstep) {
        if (superstep <= 1) {
            return;
        }
        String mergedAggregatorPath =
            getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
        JSONArray aggregatorArray = null;
        try {
            byte [] zkData =
                getZkExt().getData(mergedAggregatorPath, false, null);
            aggregatorArray = new JSONArray(new String(zkData));
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getAggregatorValues: no aggregators in " +
                     mergedAggregatorPath + " on superstep " + superstep);
            return;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Base64 base64 = new Base64();
        for (int i = 0; i < aggregatorArray.length(); ++i) {
            try {
                LOG.info("getAggregatorValues: " +
                         "Getting aggregators from " +
                         aggregatorArray.getJSONObject(i));
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
                        (byte[]) base64.decode(aggregatorArray.getJSONObject(i).
                            getString(AGGREGATOR_VALUE_KEY)));
                aggregatorValue.readFields(
                    new DataInputStream(input));
                aggregator.setAggregatedValue(aggregatorValue);
                LOG.info("getAggregatorValues: " +
                         "Got aggregator=" + aggregatorName + " value=" +
                         aggregatorValue);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("getAggregatorValues: Finished loading " +
                 mergedAggregatorPath + " with aggregator values " +
                 aggregatorArray);
    }

    /**
     * Register the health of this worker for a given superstep
     *
     * @param superstep Superstep to register health on
     */
    private void registerHealth(long superstep) {
        JSONArray hostnamePort = new JSONArray();
        hostnamePort.put(getHostname());

        hostnamePort.put(finalRpcPort);

        String myHealthPath = null;
        if (isHealthy()) {
            myHealthPath = getWorkerHealthyPath(getApplicationAttempt(),
                                                getSuperstep());
        }
        else {
            myHealthPath = getWorkerUnhealthyPath(getApplicationAttempt(),
                                                  getSuperstep());
        }
        myHealthPath = myHealthPath + "/" + getHostnamePartitionId();
        try {
            myHealthZnode =
                getZkExt().createExt(myHealthPath,
                                     hostnamePort.toString().getBytes(),
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
                     " and hostnamePort = " + hostnamePort.toString());
        }
    }

    public boolean startSuperstep() {
        // Algorithm:
        // 1. Register my health for the next superstep.
        // 2. Wait until the vertex range assignment is complete (unless
        //    superstep 0).
        registerHealth(getSuperstep());

        String vertexRangeAssignmentsNode = null;
        if (getSuperstep() > 0) {
            vertexRangeAssignmentsNode =
                getVertexRangeAssignmentsPath(getApplicationAttempt(),
                                              getSuperstep());
            try {
                while (getZkExt().exists(vertexRangeAssignmentsNode, true) ==
                        null) {
                    getVertexRangeAssignmentsReadyChangedEvent().waitForever();
                    getVertexRangeAssignmentsReadyChangedEvent().reset();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("startSuperstep: Ready for computation since worker " +
                 "selection and vertex range assignments are done in " +
                 vertexRangeAssignmentsNode);

        getAggregatorValues(getSuperstep());
        return true;
    }

    @Override
    public boolean finishSuperstep(final Map<I, long []> maxIndexStatsMap) {
        // This barrier blocks until success (or the master signals it to
        // restart).
        //
        // Master will coordinate the barriers and aggregate "doneness" of all
        // the vertices.  Each worker will:
        // 1. Save aggregator values that are in use.
        // 2. Report the number of vertices in each partition on this worker
        //    and the number completed.
        // 3. Let the master know it is finished.
        // 4. Then it waits for the master to say whether to stop or not.
        JSONArray aggregatorValueArray =
            marshalAggregatorValues(getSuperstep());
        JSONArray vertexRangeStatArray = new JSONArray();
        for (Map.Entry<I, long []> entry :
            maxIndexStatsMap.entrySet()) {
            JSONObject statObject = new JSONObject();
            try {
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                ((Writable) entry.getKey()).write(output);

                statObject.put(JSONOBJ_MAX_VERTEX_INDEX_KEY,
                               outputStream.toString("UTF-8"));
                statObject.put(JSONOBJ_FINISHED_VERTICES_KEY,
                               entry.getValue()[0]);
                statObject.put(JSONOBJ_NUM_VERTICES_KEY,
                               entry.getValue()[1]);
                statObject.put(JSONOBJ_NUM_EDGES_KEY,
                               entry.getValue()[2]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            vertexRangeStatArray.put(statObject);
        }

        JSONObject workerFinishedInfoObj = new JSONObject();
        try {
            workerFinishedInfoObj.put(JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY,
                                      aggregatorValueArray);
            workerFinishedInfoObj.put(JSONOBJ_VERTEX_RANGE_STAT_ARRAY_KEY,
                                      vertexRangeStatArray);
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

        String superstepFinishedNode =
            getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
        JSONObject globalStatsObject = null;
        try {
            while (getZkExt().exists(superstepFinishedNode, true) == null) {
                getSuperstepFinishedEvent().waitForever();
                getSuperstepFinishedEvent().reset();
            }
            globalStatsObject = new JSONObject(
                new String(getZkExt().getData(superstepFinishedNode,
                                              false,
                                              null)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long finishedVertices =
            globalStatsObject.optLong(JSONOBJ_FINISHED_VERTICES_KEY);
        totalVertices =
            globalStatsObject.optLong(JSONOBJ_NUM_VERTICES_KEY);
        totalEdges =
            globalStatsObject.optLong(JSONOBJ_NUM_EDGES_KEY);
        if (LOG.isInfoEnabled()) {
            LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
                     " with total finished vertices = " + finishedVertices +
                     " of out total vertices = " + totalVertices +
                     ", total edges = " + totalEdges);
        }
        incrCachedSuperstep();
        getContext().setStatus(getBspMapper().getMapFunctions().toString() +
                               " - Attempt=" + getApplicationAttempt() +
                               ", Superstep=" + getSuperstep());
        return (finishedVertices == totalVertices);
    }

    @Override
    public long getTotalVertices() {
        return totalVertices;
    }

    @Override
    public long getTotalEdges() {
        return totalEdges;
    }

    /**
     * Save the vertices using the user-defined VertexOutputFormat from our
     * vertexArray based on the split.
     */
    private void saveVertices() throws IOException {
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
        for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
            getVertexRangeMap().entrySet()) {
            for (BasicVertex<I, V, E, M> vertex :
                entry.getValue().getVertexMap().values()) {
                vertexWriter.writeVertex(vertex);
            }
        }
        vertexWriter.close(getContext());
    }

    @Override
    public void cleanup() throws IOException {
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
    }

    @Override
    public VertexRange<I, V, E, M> getVertexRange(long superstep, I index) {
        I maxVertexIndex = getVertexRangeMap(superstep).ceilingKey(index);

        if (maxVertexIndex == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getVertexRange: no partition for " +
                          "destination vertex " +
                          index + " -- returning last partition");
            }
            return getVertexRangeMap(superstep).lastEntry().getValue();
        }
        else {
            return getVertexRangeMap(superstep).get(maxVertexIndex);
        }
    }

    @Override
    public void storeCheckpoint() throws IOException {
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

        // Remove these files if they already exist
        try {
            getFs().delete(validFilePath, false);
            LOG.warn("storeCheckpoint: Removed file " + validFilePath);
        } catch (IOException e) {
        }
        try {
            getFs().delete(metadataFilePath, false);
            LOG.warn("storeCheckpoint: Removed file " + metadataFilePath);
        } catch (IOException e) {
        }
        try {
            getFs().delete(verticesFilePath, false);
            LOG.warn("storeCheckpoint: Removed file " + verticesFilePath);
        } catch (IOException e) {
        }

        FSDataOutputStream verticesOutputStream =
            getFs().create(verticesFilePath);
        ByteArrayOutputStream metadataByteStream = new ByteArrayOutputStream();
        DataOutput metadataOutput = new DataOutputStream(metadataByteStream);
        long workerVertexRanges = 0;
        for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
                getVertexRangeMap().entrySet()) {
            // Only write out the partitions the worker is responsible for
            if (!entry.getValue().getHostnameId().equals(
                    getHostnamePartitionId())) {
                continue;
            }

            ++workerVertexRanges;
            // Write the vertices (index, data, edges and messages)
            // Format:
            // <vertex count>
            //   <v0 id><v0 value>
            //     <v0 num edges>
            //       <v0 edge 0 dest><v0 edge 0 value>
            //       <v0 edge 1 dest><v0 edge 1 value>...
            //     <v0 message count>
            //       <v0 msg 0><v0 msg 1>...
            long startPos = verticesOutputStream.getPos();
            verticesOutputStream.writeLong(
                entry.getValue().getVertexMap().size());
            for (BasicVertex<I, V, E, M> vertex :
                    entry.getValue().getVertexMap().values()) {
                ByteArrayOutputStream vertexByteStream =
                    new ByteArrayOutputStream();
                DataOutput vertexOutput =
                    new DataOutputStream(vertexByteStream);
                ((MutableVertex<I, V, E, M>) vertex).write(vertexOutput);
                verticesOutputStream.write(vertexByteStream.toByteArray());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("storeCheckpoint: Wrote vertex id = " +
                              vertex.getVertexId() + " with " +
                              vertex.getOutEdgeMap().size() + " edges and " +
                              vertex.getMsgList().size() + " messages (" +
                              vertexByteStream.size() + " total bytes)");
                }
            }
            // Write the metadata for this vertex range
            // Format:
            // <index count>
            //   <index 0 start pos><# vertices><# edges><max index 0>
            //   <index 1 start pos><# vertices><# edges><max index 1>...
            metadataOutput.writeLong(startPos);
            metadataOutput.writeLong(entry.getValue().getVertexMap().size());
            long edgeCount = 0;
            for (BasicVertex<I, V, E, M> vertex :
                    entry.getValue().getVertexMap().values()) {
                edgeCount += vertex.getOutEdgeMap().size();
            }
            metadataOutput.writeLong(edgeCount);
            entry.getKey().write(metadataOutput);
            if (LOG.isDebugEnabled()) {
                LOG.debug("storeCheckpoint: Vertex file starting " +
                          "offset = " + startPos + ", length = " +
                          (verticesOutputStream.getPos() - startPos) +
                          ", max index of vertex range = " + entry.getKey());
            }
        }
        // Metadata is buffered and written at the end since it's small and
        // needs to know how many vertex ranges this worker owns
        FSDataOutputStream metadataOutputStream =
            getFs().create(metadataFilePath);
        metadataOutputStream.writeLong(workerVertexRanges);
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

    /**
     * Load a single vertex range from checkpoint files.
     *
     * @param maxIndex denotes the vertex range
     * @param dataFileName name of the data file
     * @param startPos position to start from in data file
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private void loadVertexRange(I maxIndex,
                                 String dataFileName,
                                 long startPos)
        throws IOException, InstantiationException, IllegalAccessException {
        // Read in the reverse order from storeCheckpoint()
        DataInputStream dataStream = getFs().open(new Path(dataFileName));
        if (dataStream.skip(startPos) != startPos) {
            throw new IllegalStateException(
                "loadVertexRange: Failed to skip " + startPos);
        }
        long vertexCount = dataStream.readLong();
        VertexRange<I, V, E, M> vertexRange = getVertexRangeMap().get(maxIndex);
        for (int i = 0; i < vertexCount; ++i) {
            Vertex<I, V, E, M> vertex =
                BspUtils.<I, V, E, M>createVertex(getConfiguration());
            vertex.readFields(dataStream);
            // Add the vertex
            if (vertexRange.getVertexMap().put(vertex.getVertexId(), vertex)
                    != null) {
                throw new IllegalStateException(
                    "loadVertexRange: Vertex "  + vertex + " already exists");
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("loadVertexRange: " + vertexCount + " vertices in " +
                     dataFileName);
        }
        dataStream.close();
    }

    @Override
    public void loadCheckpoint(long superstep) {
        // Algorithm:
        // Check all the vertex ranges for this worker and load the ones
        // that match my hostname and id.
        I maxVertexIndex = BspUtils.<I>createVertexIndex(getConfiguration());
        long startPos = -1;
        long vertexRangeCount = -1;
        for (VertexRange<I, V, E, M> vertexRange :
                getVertexRangeMap().values()) {
            if (vertexRange.getHostnameId().compareTo(
                    getHostnamePartitionId()) == 0) {
                String metadataFile =
                    vertexRange.getCheckpointFilePrefix() +
                    CHECKPOINT_METADATA_POSTFIX;
                try {
                    DataInputStream metadataStream =
                        getFs().open(new Path(metadataFile));
                    vertexRangeCount = metadataStream.readLong();
                    for (int i = 0; i < vertexRangeCount; ++i) {
                        startPos = metadataStream.readLong();
                        // Skip the vertex count
                        metadataStream.readLong();
                        // Skip the edge count
                        metadataStream.readLong();
                        maxVertexIndex.readFields(metadataStream);
                        @SuppressWarnings("unchecked")
                        int compareTo =
                            vertexRange.getMaxIndex().compareTo(maxVertexIndex);
                        LOG.debug("loadCheckpoint: Comparing " +
                                  vertexRange.getMaxIndex() + " and " +
                                  maxVertexIndex + " = " + compareTo);
                        if (compareTo == 0) {
                            loadVertexRange(
                                vertexRange.getMaxIndex(),
                                vertexRange.getCheckpointFilePrefix() +
                                    CHECKPOINT_VERTICES_POSTFIX,
                                startPos);
                        }
                    }
                    metadataStream.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public final void exchangeVertexRanges() {
        boolean syncRequired = false;
        for (Entry<I, VertexRange<I, V, E, M>> entry :
                getVertexRangeMap().entrySet()) {
            final int previousPort = entry.getValue().getPreviousPort();
            final String previousHostname =
                entry.getValue().getPreviousHostname();
            final int port = entry.getValue().getPort();
            final String hostname = entry.getValue().getHostname();
            if (LOG.isDebugEnabled()) {
                LOG.debug("exchangeVertexRanges: For max index " +
                          entry.getKey() + ", count " +
                          entry.getValue().getVertexMap().size() +
                          ", has previous port " +
                          previousPort + ", previous hostname "
                          + previousHostname +
                          ", port " + port + ", hostname " + hostname);
            }
            if (previousPort == -1) {
                continue;
            }

            if ((previousPort == finalRpcPort) &&
                    getHostname().equals(previousHostname) &&
                    ((port != finalRpcPort) ||
                            !(getHostname().equals(hostname)))) {
                if (!syncRequired) {
                    getBspMapper().getWorkerCommunications().
                        cleanCachedVertexAddressMap();
                }
                List<BasicVertex<I, V, E, M>> vertexList =
                    new ArrayList<BasicVertex<I, V, E, M>>(
                        entry.getValue().getVertexMap().values());
                if (vertexList != null) {
                    LOG.info("exchangeVertexRanges: Sending vertex range " +
                             entry.getKey() + " with " +
                             vertexList.size() + " elements to " + hostname +
                             ":" + port);
                    getBspMapper().getWorkerCommunications().sendVertexListReq(
                        entry.getKey(), vertexList);
                    vertexList.clear();
                    entry.getValue().getVertexMap().clear();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("exchangeVertexRanges: Sent vertex range " +
                                 entry.getKey() + " with " +
                                 vertexList.size() +
                                 " elements to " + hostname +
                                 ":" + port + " " + vertexList.size() + " " +
                                 entry.getValue().getVertexMap().size());
                    }
                }
                syncRequired = true;
            }
            else if ((port == finalRpcPort) &&
                    getHostname().equals(hostname) &&
                    ((previousPort != finalRpcPort) ||
                            !(getHostname().equals(previousHostname)))) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("exchangeVertexRanges: Receiving " +
                             entry.getKey() + " from " +
                             previousHostname + ":" + previousPort);
                }
                if (!syncRequired) {
                    getBspMapper().getWorkerCommunications().
                        cleanCachedVertexAddressMap();
                }
                VertexRange<I, V, E, M> destVertexRange =
                    getVertexRangeMap().get(entry.getKey());
                if ((destVertexRange.getVertexMap() != null) &&
                        !destVertexRange.getVertexMap().isEmpty()) {
                    throw new RuntimeException(
                        "exchangeVertexRanges: Cannot receive max index " +
                        entry.getKey() + " since already have " +
                        destVertexRange.getVertexMap().size() + " elements.");
                }
                syncRequired = true;
            }
        }

        // All senders and receivers must agree they are finished
        if (syncRequired) {
            String myVertexRangeExchangePath =
                getVertexRangeExchangePath(getApplicationAttempt(),
                                           getSuperstep()) +
                                           "/" + getHostnamePartitionId();
            String vertexRangeExchangeFinishedPath =
                getVertexRangeExchangeFinishedPath(getApplicationAttempt(),
                                                   getSuperstep());
            LOG.info("exchangeVertexRanges: Ready with path " +
                     myVertexRangeExchangePath);
            try {
                getZkExt().createExt(myVertexRangeExchangePath,
                                     null,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
                LOG.info("exchangeVertexRanges: Waiting on change to " +
                         vertexRangeExchangeFinishedPath);
                while (getZkExt().exists(vertexRangeExchangeFinishedPath, true)
                        == null) {
                    getVertexRangeExchangeFinishedChangedEvent().waitForever();
                    getVertexRangeExchangeFinishedChangedEvent().reset();
                }
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Add the vertices that were sent earlier.
        Map<I, List<Vertex<I, V, E, M>>> inVertexRangeMap =
            getBspMapper().getWorkerCommunications().getInVertexRangeMap();
        synchronized (inVertexRangeMap) {
            for (Entry<I, List<Vertex<I, V, E, M>>> entry :
                    inVertexRangeMap.entrySet()) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }

                SortedMap<I, BasicVertex<I, V, E, M>> vertexMap =
                    getVertexRangeMap().get(entry.getKey()).getVertexMap();
                if (vertexMap.size() != 0) {
                    throw new RuntimeException(
                        "exchangeVertexRanges: Failed to import vertex range " +
                        entry.getKey() + " of size " + entry.getValue().size() +
                        " since it is already of size " + vertexMap.size() +
                        " but should be empty!");
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("exchangeVertexRanges: Adding " +
                             entry.getValue().size() +
                             " vertices for max index " + entry.getKey());
                }
                for (BasicVertex<I, V, E, M> vertex : entry.getValue()) {
                    if (vertexMap.put(vertex.getVertexId(), vertex) != null) {
                        throw new IllegalStateException(
                            "exchangeVertexRanges: Vertex " + vertex +
                            " already exists!");
                    }
                }
                entry.getValue().clear();
            }
        }
    }

    @Override
    public NavigableMap<I, VertexRange<I, V, E, M>> getVertexRangeMap() {
        return getVertexRangeMap(getSuperstep());
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
                if ((State.valueOf(jsonObj.getString(JSONOBJ_STATE_KEY)) ==
                        State.START_SUPERSTEP) &&
                        jsonObj.getLong(JSONOBJ_APPLICATION_ATTEMPT_KEY) !=
                        getApplicationAttempt() &&
                        getSuperstep() > 0) {
                    LOG.fatal("processEvent: Worker will restart " +
                              "from command - " + jsonObj.toString());
                    System.exit(-1);
                }
            } catch (JSONException e) {
                throw new RuntimeException(
                    "processEvent: Couldn't properly get job state from " +
                    jsonObj.toString());
            }
            return true;
        }
        return foundEvent;
    }
}
