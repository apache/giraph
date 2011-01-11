package com.yahoo.hadoop_bsp;

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
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceWorker}.
 * @author aching
 */
public class BspServiceWorker<
    I extends WritableComparable, V, E, M extends Writable>
    extends BspService<I, V, E, M> implements
    CentralizedServiceWorker<I, V, E, M> {
    /** The partition map */
    private NavigableSet<Partition<I>> m_partitionSet = null;
    /** Number of input splits */
    private int m_inputSplitCount = -1;
    /** Cached aggregate number of vertices in the entire application */
    long m_totalVertices = -1;
    /** My process health znode */
    private String m_myHealthZnode;
    /** Partition to compare with (saved local variable to reduce allocation) */
    private Partition<I> comparePartition = new Partition<I>("", -1, null);
    /** Data structure or storing each range and associated vertices */
    private Map<I, List<Vertex<I, V, E, M>>> m_maxIndexVertexMap =
        new TreeMap<I, List<Vertex<I, V, E, M>>>();
    /** Map of aggregators */
    private static Map<String, Aggregator<Writable>> m_aggregatorMap =
        new TreeMap<String, Aggregator<Writable>>();
    /** List of aggregators currently in use */
    private static Set<String> m_aggregatorInUse =
        new TreeSet<String>();
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);

    public BspServiceWorker(String serverPortList,
                            int sessionMsecTimeout,
                            @SuppressWarnings("rawtypes") Context context,
                            BspJob.BspMapper<I, V, E, M> bspMapper) {
        super(serverPortList, sessionMsecTimeout, context, bspMapper);
    }

    /**
     * Intended to check the health of the node.  For instance, can it ssh,
     * dmesg, etc. For now, does nothing.
     */
    public boolean isHealthy() {
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
            if (m_inputSplitCount == -1) {
                m_inputSplitCount = inputSplitPathList.size();
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
            LOG.info("reserveInputSplit: reservedPath = " +
                     reservedInputSplitPath + ", " + finishedInputSplits +
                     " of " + inputSplitPathList.size() +
                     " InputSplits are finished.");
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
     * Each worker will set the partitions that it has found for a given
     * InputSplit. After this, the InputSplit is considered finished.
     *
     * @param inputSplitPath path to the input split znode
     * @param maxIndexCoundMap maps max vertex indexes to the number of vertices
     *        in each partition (can be null, where nothing is written)
     */
    private void setInputSplitVertexRanges(String inputSplitPath,
                                           Map<I, Long> maxIndexCountMap) {
        String inputSplitFinishedPath =
            inputSplitPath + INPUT_SPLIT_FINISHED_NODE;
        byte [] zkData = null;
        JSONArray statArray = new JSONArray();
        if (maxIndexCountMap != null) {
            for (Map.Entry<I, Long> entry : maxIndexCountMap.entrySet()) {
                try {
                    ByteArrayOutputStream outputStream =
                        new ByteArrayOutputStream();
                    DataOutput output = new DataOutputStream(outputStream);
                    ((Writable) entry.getKey()).write(output);

                    JSONObject vertexRangeObj = new JSONObject();
                    vertexRangeObj.put(STAT_NUM_VERTICES_KEY,
                                       entry.getValue());
                    vertexRangeObj.put(STAT_HOSTNAME_ID_KEY,
                                       getHostnamePartitionId());
                    vertexRangeObj.put(STAT_MAX_INDEX_KEY,
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
     */
    private void loadVertices() {
        List<HadoopVertex<I, V, E, M>> vertexList =
            new ArrayList<HadoopVertex<I, V, E, M>>();
        String inputSplitPath = null;
        while ((inputSplitPath = reserveInputSplit()) != null) {
            @SuppressWarnings("unchecked")
            Class<? extends Writable> inputSplitClass =
                (Class<Writable>) getConfiguration().getClass("bsp.inputSplitClass",
                                                  InputSplit.class);
            InputSplit inputSplit = (InputSplit)
                ReflectionUtils.newInstance(inputSplitClass, getConfiguration());
            byte[] splitList;
            try {
                splitList = getZkExt().getData(inputSplitPath, false, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("loadVertices: Reserved " + inputSplitPath +
                     " and got '" + splitList + "'");
            getContext().progress();

            InputStream input =
                new ByteArrayInputStream(splitList);
            try {
                ((Writable) inputSplit).readFields(new DataInputStream(input));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            @SuppressWarnings("unchecked")
            Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
                (Class<? extends VertexInputFormat<I, V, E>>)
                getConfiguration().getClass("bsp.vertexInputFormatClass",
                                VertexInputFormat.class);
            @SuppressWarnings("rawtypes")
            Class<? extends HadoopVertex> vertexClass =
                getConfiguration().getClass("bsp.vertexClass",
                                HadoopVertex.class,
                                HadoopVertex.class);
            VertexInputFormat<I, V, E> vertexInputFormat = null;
            VertexReader<I, V, E> vertexReader = null;
            try {
                vertexInputFormat = vertexInputFormatClass.newInstance();
                vertexReader =
                    vertexInputFormat.createVertexReader(inputSplit, getContext());
                vertexReader.initialize(inputSplit, getContext());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            vertexList.clear();
            try {
                @SuppressWarnings("unchecked")
                HadoopVertex<I, V, E, M> vertex = vertexClass.newInstance();
                while (vertexReader.next(vertex)) {
                    vertex.setBspMapper(getBspMapper());
                    vertexList.add(vertex);
                    @SuppressWarnings("unchecked")
                    HadoopVertex<I, V, E, M> newInstance =
                        vertexClass.newInstance();
                    vertex = newInstance;
                    getContext().progress();
                }
                vertexReader.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (vertexList.isEmpty()) {
                LOG.info("loadVertices: No vertices in input split " +
                         inputSplit);
                // TODO: Need to add checkpoints
                setInputSplitVertexRanges(inputSplitPath, null);
                continue;
            }

            // Separate all the vertices in this InputSplit into vertex ranges.
            // The number of vertex ranges is up to half of the number of
            // available workers and must reach a minimum size.  Then two passes
            // over the vertexList.  First, find the maximum vertex ranges.
            // Then fill them in.
            long partitionsPerInputSplit = (long) (m_inputSplitCount *
                getConfiguration().getFloat(
                    BspJob.BSP_TOTAL_INPUT_SPLIT_MULTIPLIER,
                    BspJob.DEFAULT_BSP_TOTAL_INPUT_SPLIT_MULTIPLIER));
            if (partitionsPerInputSplit == 0) {
                partitionsPerInputSplit = 1;
            }
            long vertexRangeSize = vertexList.size() / partitionsPerInputSplit;
            long minPerVertexRange =
                getConfiguration().getLong(
                    BspJob.BSP_MIN_VERTICES_PER_RANGE,
                    BspJob.DEFAULT_BSP_MIN_VERTICES_PER_RANGE);
            if (vertexRangeSize < minPerVertexRange) {
                vertexRangeSize = minPerVertexRange;
            }
            I vertexIdMax = null;
            for (int i = 0; i < vertexList.size(); ++i) {
                if ((vertexIdMax != null) && ((i % vertexRangeSize) == 0)) {
                    m_maxIndexVertexMap.put(
                        vertexIdMax,
                        new ArrayList<Vertex<I, V, E, M>>());
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
            m_maxIndexVertexMap.put(vertexIdMax,
                                    new ArrayList<Vertex<I,V,E,M>>());

            Iterator<I> maxIndexVertexMapIt =
                m_maxIndexVertexMap.keySet().iterator();
            I currentMaxIndex = maxIndexVertexMapIt.next();
            for (HadoopVertex<I, V, E, M> vertex : vertexList) {
                @SuppressWarnings("unchecked")
                int compareTo =
                    vertex.getVertexId().compareTo(currentMaxIndex);
                if (compareTo > 0) {
                    if (!maxIndexVertexMapIt.hasNext()) {
                        throw new RuntimeException(
                            "loadVertices: Impossible that vertex " +
                            vertex.getVertexId() + " > " + currentMaxIndex);
                    }
                    currentMaxIndex = maxIndexVertexMapIt.next();
                }
                LOG.debug("loadVertices: Adding vertex with index = " +
                          vertex.getVertexId() + " to vertex range max = " +
                          currentMaxIndex);
                m_maxIndexVertexMap.get(currentMaxIndex).add(vertex);
            }
            Map<I, Long> maxIndexCountMap = new TreeMap<I, Long>();
            for (Map.Entry<I, List<Vertex<I, V, E, M>>> entry :
                m_maxIndexVertexMap.entrySet()) {
                maxIndexCountMap.put(entry.getKey(),
                                     new Long(entry.getValue().size()));
            }
            // TODO: Add checkpoint
            setInputSplitVertexRanges(inputSplitPath, maxIndexCountMap);
        }
    }

    public void setup() {
        //
        // Prepare for computation:
        // 1. Start superstep 0 (no computation)
        // 2. Wait for the INPUT_SPLIT_READY_PATH node has been created
        // 3. Process input splits until there are no more.
        // 4. Wait for superstep 0 to complete.

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
        loadVertices();

        Map<I, long []> maxIndexStatsMap = new TreeMap<I, long []>();
        for (Map.Entry<I, List<Vertex<I, V, E, M>>> entry :
             getMaxIndexVertexLists().entrySet()) {
            long [] statArray = new long[2];
            statArray[0] = 0;
            statArray[1] = entry.getValue().size();
            maxIndexStatsMap.put(entry.getKey(), statArray);
        }

        finishSuperstep(maxIndexStatsMap);
    }

    /**
     * Save values of aggregators in use to be aggregated by master.
     *
     * @param superstep
     */
    private void sendAggregatorValues(long superstep) {
        if (superstep == 0 || m_aggregatorInUse.size() == 0) {
            return;
        }
        String aggregatorWorkerPath =
            getAggregatorWorkerPath(getApplicationAttempt(), superstep);
        byte [] zkData = null;
        JSONArray aggregatorArray = new JSONArray();
        Base64 base64 = new Base64();
        for (String name : m_aggregatorInUse) {
            try {
                Aggregator<Writable> aggregator = m_aggregatorMap.get(name);
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                aggregator.getAggregatedValue().write(output);

                JSONObject aggregatorObj = new JSONObject();
                aggregatorObj.put(AGGREGATOR_NAME_KEY, name);
                aggregatorObj.put(AGGREGATOR_VALUE_KEY,
                       base64.encodeToString(outputStream.toByteArray()));
                aggregatorArray.put(aggregatorObj);
                LOG.info("sendAggregatorValues: " +
                         "Trying to add aggregatorObj " +
                         aggregatorObj + "(" + 
                         aggregator.getAggregatedValue() + ") to aggregator path " +
                         aggregatorWorkerPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        zkData = aggregatorArray.toString().getBytes();
        try {
            getZkExt().createExt(aggregatorWorkerPath,
                                 zkData,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("sendAggregatorValues: " + aggregatorWorkerPath +
                     " already exists!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("sendAggregatorValues: Finished loading " +
                 aggregatorWorkerPath + " with aggregator values " + aggregatorArray);
        m_aggregatorInUse.clear();
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
        String aggregatorPath =
            getAggregatorPath(getApplicationAttempt(), superstep-1);
        JSONArray aggregatorArray = null;
        try {
            byte [] zkData =
                getZkExt().getData(aggregatorPath, false, null);
            aggregatorArray = new JSONArray(new String(zkData));
        } catch (KeeperException.NoNodeException e) {
            LOG.info("getAggregatorValues: no aggregators in " +
                     aggregatorPath);
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
                Aggregator<Writable> aggregator = m_aggregatorMap.get(aggregatorName);
                Writable aggregatorValue = aggregator.getAggregatedValue();
                InputStream input =
                    new ByteArrayInputStream(
                        base64.decode(aggregatorArray.getJSONObject(i).
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
                 aggregatorPath + " with aggregator values " + aggregatorArray);
    }

    public Map<I, List<Vertex<I, V, E, M>>> getMaxIndexVertexLists() {
        return m_maxIndexVertexMap;
    }

    public boolean startSuperstep() {
        // Register my health for the next superstep and wait until the worker
        // selection is complete
        JSONArray hostnamePort = new JSONArray();
        hostnamePort.put(getHostname());
        int finalRpcPort =
            getConfiguration().getInt(BspJob.BSP_RPC_INITIAL_PORT,
                          BspJob.DEFAULT_BSP_RPC_INITIAL_PORT) +
                          getTaskPartition();
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
            m_myHealthZnode =
                getZkExt().createExt(myHealthPath,
                                     hostnamePort.toString().getBytes(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.EPHEMERAL,
                                     true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("startSuperstep: myHealthPath already exists (likely from " +
                     "previous failure): " + myHealthPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("startSuperstep: Created my health node for attempt=" +
                 getApplicationAttempt() + ", superstep=" +
                 getSuperstep() + " with " + m_myHealthZnode +
                 " and hostnamePort = " + hostnamePort.toString());

        String workerSelectionFinishedNode =
            getWorkerSelectionFinishedPath(getApplicationAttempt(),
                                           getSuperstep());
        try {
            while (getZkExt().exists(workerSelectionFinishedNode, true) ==
                   null) {
                getWorkerPartitionsAllReadyChangedEvent().waitForever();
                getWorkerPartitionsAllReadyChangedEvent().reset();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("startSuperstep: Ready for computation (got " +
                 workerSelectionFinishedNode + ")");

        getAggregatorValues(getSuperstep());
        return true;
    }

    public boolean finishSuperstep(final Map<I, long []> maxIndexStatsMap) {
        // TODO: Note that this barrier blocks until success.  It would be
        // best if it were interruptible if for instance there was a failure.

        // Master will coordinate the barriers and aggregate "doneness" of all
        // the vertices.  Each worker will:
        // 1. Save aggregator values that are in use.
        // 2. Report the number of vertices in each partition on this worker
        //    and the number completed.
        // 3. Let the master know it is finished.
        // 4. Then it waits for the master to say whether to stop or not.

        sendAggregatorValues(getSuperstep());
        String myVertexRangeStatPath =
            getVertexRangeStatsPath(getApplicationAttempt(), getSuperstep());
        JSONArray allStatArray = new JSONArray();
        for (Map.Entry<I, long []> entry :
            maxIndexStatsMap.entrySet()) {
            JSONObject statObject = new JSONObject();
            try {
                ByteArrayOutputStream outputStream =
                    new ByteArrayOutputStream();
                DataOutput output = new DataOutputStream(outputStream);
                ((Writable) entry.getKey()).write(output);

                statObject.put(STAT_MAX_INDEX_KEY,
                               outputStream.toString("UTF-8"));
                statObject.put(STAT_FINISHED_VERTICES_KEY,
                               entry.getValue()[0]);
                statObject.put(STAT_NUM_VERTICES_KEY,
                               entry.getValue()[1]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            allStatArray.put(statObject);
        }
        try {
            getZkExt().createExt(myVertexRangeStatPath,
                                 allStatArray.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("finishSuperstep: Failed to write stats " + allStatArray +
                     " into " + myVertexRangeStatPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String finishedWorkerPath =
            getWorkerFinishedPath(getApplicationAttempt(), getSuperstep()) +
            "/" + getHostnamePartitionId();
        try {
            getZkExt().createExt(finishedWorkerPath,
                                 null,
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
                new String(getZkExt().getData(superstepFinishedNode, false, null)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long finishedVertices =
            globalStatsObject.optLong(STAT_FINISHED_VERTICES_KEY);
        m_totalVertices =
            globalStatsObject.optLong(STAT_NUM_VERTICES_KEY);
        LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
                 " with finishedVertices=" + finishedVertices +
                 ", numVertices=" + m_totalVertices);
        incrSuperstep();
        return (finishedVertices == m_totalVertices);
    }

    public long getTotalVertices() {
        return m_totalVertices;
    }

    /**
     * Save the vertices using the user-defined OutputFormat from our
     * vertexArray based on the split.
     */
    public void saveVertices() {
        if (getConfiguration().get("bsp.vertexWriterClass") == null) {
            LOG.warn("saveVertices: bsp.vertexWriterClass not specified" +
            " -- there will be no saved output");
            return;
        }

        @SuppressWarnings("unchecked")
        Class<? extends VertexWriter<I, V, E>> vertexWriterClass =
            (Class<? extends VertexWriter<I, V, E>>)
            getConfiguration().getClass("bsp.vertexWriterClass",
                                        VertexWriter.class);
        VertexWriter<I, V, E> vertexWriter = null;
        try {
            vertexWriter = vertexWriterClass.newInstance();
            for (Map.Entry<I, List<Vertex<I, V, E, M>>> entry :
                getMaxIndexVertexLists().entrySet()) {
                for (Vertex<I, V, E, M> vertex : entry.getValue()) {
                    vertexWriter.write(getContext(),
                                       vertex.getVertexId(),
                                       vertex.getVertexValue(),
                                       vertex.getOutEdgeIterator());
                }
            }
            vertexWriter.close(getContext());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void cleanup() {
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
             LOG.info("cleanup: Notifying master its okay to cleanup with " +
                     finalFinishedPath);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("cleanup: Couldn't create finished node '" +
                     cleanedUpPath);
        } catch (Exception e) {
            // cleanup phase -- just log the error
            LOG.error(e.getMessage());
        }
        try {
            getZkExt().close();
        } catch (InterruptedException e) {
            // cleanup phase -- just log the error
            LOG.error("cleanup: Zookeeper failed to close with " + e);
        }
    }

    /**
     * Parse the good workers in this superstep and get the mapping of hostname +
     * partition to hostname and port.
     *
     * @return
     */
    private Map<String, JSONArray> getWorkerHostPortMap() {
        List<String> healthyWorkerList = null;
        String workerHealthyPath =
            getWorkerHealthyPath(getApplicationAttempt(),
                                 getSuperstep());
        try {
            getZkExt().createExt(workerHealthyPath,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("getWorkerHostPortMap: Node " + workerHealthyPath +
                     " already exists, not creating");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            healthyWorkerList =
                getZkExt().getChildrenExt(
                    workerHealthyPath, false, false, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Map<String, JSONArray> workerHostPortMap =
            new TreeMap<String, JSONArray>();
        for (String healthyWorker : healthyWorkerList) {
            JSONArray hostnamePortArray = null;
            try {
                String hostnamePortString =
                    new String(getZkExt().getData(
                        workerHealthyPath + "/" + healthyWorker, false, null));
                LOG.info("getWorkerHostPortMap: Got " + hostnamePortString +
                         " from " + healthyWorker);
                hostnamePortArray = new JSONArray(hostnamePortString);
                if (hostnamePortArray.length() != 2) {
                    throw new RuntimeException(
                        "getWorkerHostPortMap: Impossible that znode " +
                        healthyWorker + " has jsonArray " + hostnamePortArray);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            workerHostPortMap.put(healthyWorker, hostnamePortArray);
        }
        return workerHostPortMap;
    }

    public NavigableSet<Partition<I>> getPartitionSet() {
        if (m_partitionSet != null) {
            return m_partitionSet;
        }

        m_partitionSet = new TreeSet<Partition<I>>();
        Map<String, JSONArray> workerHostPortMap = getWorkerHostPortMap();
        String workerSelectedPath =
            getWorkerSelectedPath(getApplicationAttempt(),
                                  getSuperstep());
        List<String> workerSelectedList = null;
        try {
            workerSelectedList =
                getZkExt().getChildrenExt(workerSelectedPath, false, false, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        @SuppressWarnings({"rawtypes", "unchecked" })
        Class<? extends WritableComparable> indexClass =
            (Class<? extends WritableComparable>)
            getConfiguration().getClass("bsp.indexClass",
                                        WritableComparable.class);
        for (String worker : workerSelectedList) {
            try {
                JSONArray maxIndexArray =
                    new JSONArray(
                        new String(
                            getZkExt().getData(workerSelectedPath + "/" + worker,
                                         false,
                                         null)));
                LOG.info("getPartitionSet: Got partitions " +
                         maxIndexArray + " from " + worker);
                for (int i = 0; i < maxIndexArray.length(); ++i) {
                    if (!workerHostPortMap.containsKey(worker)) {
                        throw new RuntimeException(
                            "getPartitionSet:  Couldn't find worker " +
                            worker);
                    }
                    JSONArray hostPortArray = workerHostPortMap.get(worker);
                    @SuppressWarnings("unchecked")
                    I index = (I) indexClass.newInstance();
                    LOG.info("getPartitionSet: Getting max index from " +
                             maxIndexArray.getString(i));
                    InputStream input =
                        new ByteArrayInputStream(
                            maxIndexArray.getString(i).getBytes("UTF-8"));
                    ((Writable) index).readFields(new DataInputStream(input));
                    m_partitionSet.add(new Partition<I>(
                            hostPortArray.getString(0),
                            hostPortArray.getInt(1),
                            index));
                    LOG.info("getPartitionSet: Found partition: " +
                             hostPortArray.getString(0) + ":" +
                             hostPortArray.getInt(1) + "withMaxIndex=" +
                             index);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return m_partitionSet;
    }

    public Partition<I> getPartition(I index) {
        if (m_partitionSet == null) {
            getPartitionSet();
        }
        comparePartition.setMaxIndex(index);
        Partition<I> result = m_partitionSet.ceiling(comparePartition);
        if (result == null) {
            LOG.warn("getPartition: no partition for destination vertex " +
                     index + " -- returning last partition");
            result = m_partitionSet.last();
        }
        return result;
    }

    /**
     * Register an aggregator with name.
     *
     * @param name
     * @param aggregator
     * @return boolean (false when aggregator already registered)
     */
    public static <A extends Writable> boolean registerAggregator(String name,
                                Aggregator<A> aggregator) {
        if (m_aggregatorMap.get(name) != null) {
            return false;
        }
        m_aggregatorMap.put(name, (Aggregator<Writable>)aggregator);
        LOG.info("registered aggregator=" + name);
        return true;
    }

    /**
     * Get aggregator by name.
     *
     * @param name
     * @return Aggregator<A> (null when not registered)
     */
    public static <A extends Writable> Aggregator<A> getAggregator(String name) {
        return (Aggregator<A>)m_aggregatorMap.get(name);
    }

    /**
     * Use an aggregator in this superstep.
     *
     * @param name
     * @return boolean (false when aggregator not registered)
     */
    public static boolean useAggregator(String name) {
        if (m_aggregatorMap.get(name) == null) {
            LOG.error("Aggregator=" + name + " not registered");
            return false;
        }
        m_aggregatorInUse.add(name);
        return true;
    }   

}
