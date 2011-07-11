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

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.RPCCommunications;
import org.apache.giraph.comm.ServerInterface;
import org.apache.giraph.comm.WorkerCommunications;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, our job should not have any reduce tasks.
 */
public class GiraphJob extends Job {
    /** Vertex class - required */
    public static final String VERTEX_CLASS = "giraph.vertexClass";
    /** VertexInputFormat class - required */
    public static final String VERTEX_INPUT_FORMAT_CLASS =
        "giraph.vertexInputFormatClass";

    /** VertexOutputFormat class - optional */
    public static final String VERTEX_OUTPUT_FORMAT_CLASS =
        "giraph.vertexOutputFormatClass";
    /** Vertex combiner class - optional */
    public static final String VERTEX_COMBINER_CLASS =
        "giraph.combinerClass";
    /** Vertex range balancer class - optional */
    public static final String VERTEX_RANGE_BALANCER_CLASS =
        "giraph.vertexRangeBalancerClass";
    /** Vertex resolver class - optional */
    public static final String VERTEX_RESOLVER_CLASS =
        "giraph.vertexResolverClass";

    /** Vertex index class */
    public static final String VERTEX_INDEX_CLASS = "giraph.vertexIndexClass";
    /** Vertex value class */
    public static final String VERTEX_VALUE_CLASS = "giraph.vertexValueClass";
    /** Edge value class */
    public static final String EDGE_VALUE_CLASS = "giraph.edgeValueClass";
    /** Message value class */
    public static final String MESSAGE_VALUE_CLASS = "giraph.messageValueClass";

    /**
     * Minimum number of simultaneous workers before this job can run (int)
     */
    public static final String MIN_WORKERS = "giraph.minWorkers";
    /**
     * Maximum number of simultaneous worker tasks started by this job (int).
     */
    public static final String MAX_WORKERS = "giraph.maxWorkers";

    /**
     * Separate the workers and the master tasks.  This is required
     * to support dynamic recovery. (boolean)
     */
    public static final String SPLIT_MASTER_WORKER =
        "giraph.SplitMasterWorker";
    /**
     * Default on whether to separate the workers and the master tasks.
     * Needs to be "true" to support dynamic recovery.
     */
    public static final boolean SPLIT_MASTER_WORKER_DEFAULT = true;

    /**
     * Minimum percent of the maximum number of workers that have responded
     * in order to continue progressing. (float)
     */
    public static final String MIN_PERCENT_RESPONDED =
        "giraph.minPercentResponded";
    /** Default 100% response rate for workers */
    public static final float MIN_PERCENT_RESPONDED_DEFAULT = 100.0f;

    /** Polling timeout to check on the number of responded tasks (int) */
    public static final String POLL_MSECS = "giraph.pollMsecs";
    /** Default poll msecs (30 seconds) */
    public static final int POLL_MSECS_DEFAULT = 30*1000;

    /**
     *  ZooKeeper comma-separated list (if not set,
     *  will start up ZooKeeper locally)
     */
    public static final String ZOOKEEPER_LIST = "giraph.zkList";

    /** ZooKeeper session millisecond timeout */
    public static final String ZOOKEEPER_SESSION_TIMEOUT =
        "giraph.zkSessionMsecTimeout";
    /** Default Zookeeper session millisecond timeout */
    public static final int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 60*1000;

    /** Polling interval to check for the final ZooKeeper server data */
    public static final String ZOOKEEPER_SERVERLIST_POLL_MSECS =
        "giraph.zkServerlistPollMsecs";
    /** Default polling interval to check for the final ZooKeeper server data */
    public static final int ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT =
        3*1000;

    /** Number of nodes (not tasks) to run Zookeeper on */
    public static final String ZOOKEEPER_SERVER_COUNT =
        "giraph.zkServerCount";
    /** Default number of nodes to run Zookeeper on */
    public static final int ZOOKEEPER_SERVER_COUNT_DEFAULT = 1;

    /** ZooKeeper port to use */
    public static final String ZOOKEEPER_SERVER_PORT =
        "giraph.zkServerPort";
    /** Default ZooKeeper port to use */
    public static final int ZOOKEEPER_SERVER_PORT_DEFAULT = 22181;

    /** Location of the ZooKeeper jar - Used internally, not meant for users */
    public static final String ZOOKEEPER_JAR = "giraph.zkJar";

    /** Local ZooKeeper directory to use */
    public static final String ZOOKEEPER_DIR = "giraph.zkDir";

    /** Initial port to start using for the RPC communication */
    public static final String RPC_INITIAL_PORT = "giraph.rpcInitialPort";
    /** Default port to start using for the RPC communication */
    public static final int RPC_INITIAL_PORT_DEFAULT = 30000;

    /** Maximum number of RPC handlers */
    public static final String RPC_NUM_HANDLERS = "giraph.rpcNumHandlers";
    /** Default maximum number of RPC handlers */
    public static final int RPC_NUM_HANDLERS_DEFAULT = 20;

    /** Maximum number of messages per peer before flush */
    public static final String MSG_SIZE = "giraph.msgSize";
    /** Default maximum number of messages per peer before flush */
    public static final int MSG_SIZE_DEFAULT = 1000;

    /** Number of poll attempts prior to failing the job (int) */
    public static final String POLL_ATTEMPTS = "giraph.pollAttempts";
    /** Default poll attempts */
    public static final int POLL_ATTEMPTS_DEFAULT = 5;

    /** Number of minimum vertices in each vertex range */
    public static final String MIN_VERTICES_PER_RANGE =
        "giraph.minVerticesPerRange";
    /** Default number of minimum vertices in each vertex range */
    public static final long MIN_VERTICES_PER_RANGE_DEFAULT = 3;

    /**
     * Set the multiplicative factor of how many partitions to create from
     * a single InputSplit based on the number of total InputSplits.  For
     * example, if there are 10 total InputSplits and this is set to 0.5, then
     * you will get 0.5 * 10 = 5 partitions for every InputSplit (given that the
     * minimum size is met).
     */
    public static final String TOTAL_INPUT_SPLIT_MULTIPLIER =
        "giraph.totalInputSplitMultiplier";
    /** Default total input split multiplier */
    public static final float TOTAL_INPUT_SPLIT_MULTIPLIER_DEFAULT = 0.5f;

    /** Java opts passed to ZooKeeper startup */
    public static final String ZOOKEEPER_JAVA_OPTS =
        "giraph.zkJavaOpts";
    /** Default java opts passed to ZooKeeper startup */
    public static final String ZOOKEEPER_JAVA_OPTS_DEFAULT =
        "-Xmx128m";

    /**
     *  How often to checkpoint (i.e. 1 means every superstep, 2 is every
     *  two supersteps, etc.).
     */
    public static final String CHECKPOINT_FREQUENCY =
        "giraph.checkpointFrequency";

    /** Default checkpointing frequency of every 2 supersteps. */
    public static final int CHECKPOINT_FREQUENCY_DEFAULT = 2;

    /**
     * Delete checkpoints after a successful job run?
     */
    public static final String CLEANUP_CHECKPOINTS_AFTER_SUCCESS =
        "giraph.cleanupCheckpointsAfterSuccess";
    /** Default is to clean up the checkponts after a successful job */
    public static final boolean CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT =
        true;

    /**
     * An application can be restarted manually by selecting a superstep.  The
     * corresponding checkpoint must exist for this to work.  The user should
     * set a long value.  Default is start from scratch.
     */
    public static final String RESTART_SUPERSTEP = "giraph.restartSuperstep";

    /**
     * If ZOOKEEPER_LIST is not set, then use this directory to manage
     * ZooKeeper
     */
    public static final String ZOOKEEPER_MANAGER_DIRECTORY =
        "giraph.zkManagerDirectory";
    /**
     * Default ZooKeeper manager directory (where determining the servers in
     * HDFS files will go).  Final directory path will also have job number
     * for uniqueness.
     */
    public static final String ZOOKEEPER_MANAGER_DIR_DEFAULT =
        "_bsp/_defaultZkManagerDir";

    /** This directory has/stores the available checkpoint files in HDFS. */
    public static final String CHECKPOINT_DIRECTORY =
        "giraph.checkpointDirectory";
    /**
     * Default checkpoint directory (where checkpoing files go in HDFS).  Final
     * directory path will also have the job number for uniqueness
     */
    public static final String CHECKPOINT_DIRECTORY_DEFAULT =
        "_bsp/_checkpoints/";

    /** Keep the zookeeper output for debugging? Default is to remove it. */
    public static final String KEEP_ZOOKEEPER_DATA =
        "giraph.keepZooKeeperData";
    /** Default is to remove ZooKeeper data. */
    public static final Boolean KEEP_ZOOKEEPER_DATA_DEFAULT = false;

    /** Default ZooKeeper tick time. */
    public static final int DEFAULT_ZOOKEEPER_TICK_TIME = 2000;
    /** Default ZooKeeper init limit (in ticks). */
    public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;
    /** Default ZooKeeper sync limit (in ticks). */
    public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;
    /** Default ZooKeeper snap count. */
    public static final int DEFAULT_ZOOKEEPER_SNAP_COUNT = 50000;
    /** Default ZooKeeper maximum client connections. */
    public static final int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 10000;
    /** Default ZooKeeper minimum session timeout (in msecs). */
    public static final int DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT = 10000;
    /** Default ZooKeeper maximum session timeout (in msecs). */
    public static final int DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT = 100000;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GiraphJob.class);

    /**
     * Constructor that will instantiate the configuration
     *
     * @param jobName User-defined job name
     * @throws IOException
     */
    public GiraphJob(String jobName) throws IOException {
        super(new Configuration(), jobName);
    }

    /**
     * Constructor.
     *
     * @param conf User-defined configuration
     * @param jobName User-defined job name
     * @throws IOException
     */
    public GiraphJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
    }

    /**
     * Make sure the configuration is set properly by the user prior to
     * submitting the job.
     */
    private void checkConfiguration() {
        if (conf.getInt(MAX_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + MAX_WORKERS);
        }
        if (conf.getFloat(MIN_PERCENT_RESPONDED,
                          MIN_PERCENT_RESPONDED_DEFAULT) <= 0.0f ||
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) > 100.0f) {
            throw new IllegalArgumentException(
                "Invalid " +
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) + " for " +
                MIN_PERCENT_RESPONDED);
        }
        if (conf.getInt(MIN_WORKERS, -1) < 0) {
            throw new IllegalArgumentException("No valid " + MIN_WORKERS);
        }
        if (BspUtils.getVertexClass(getConfiguration()) == null) {
            throw new IllegalArgumentException("GiraphJob: Null VERTEX_CLASS");
        }
        if (BspUtils.getVertexInputFormatClass(getConfiguration()) == null) {
            throw new IllegalArgumentException(
                "GiraphJob: Null VERTEX_INPUT_FORMAT_CLASS");
        }
        if (BspUtils.getVertexResolverClass(getConfiguration()) == null) {
            setVertexResolverClass(VertexResolver.class);
            if (LOG.isInfoEnabled()) {
                LOG.info("GiraphJob: No class found for " +
                         VERTEX_RESOLVER_CLASS + ", defaulting to " +
                         VertexResolver.class.getCanonicalName());
            }
        }
    }

    /**
     * The mapper that will execute the BSP tasks.  Since this mapper will
     * not be passing data by key-value pairs through the MR framework, the
     * types are irrelevant.
     */
    @SuppressWarnings("rawtypes")
    public static class BspMapper<I extends WritableComparable,
                                  V extends Writable,
                                  E extends Writable,
                                  M extends Writable>
            extends Mapper<Object, Object, Object, Object> {
        /** Logger */
        private static final Logger LOG = Logger.getLogger(BspMapper.class);
        /** Coordination service worker */
        CentralizedServiceWorker<I, V, E, M> serviceWorker;
        /** Coordination service master thread */
        Thread masterThread = null;
        /** Communication service */
        private ServerInterface<I, V, E, M> commService = null;
        /** The map should be run exactly once, or else there is a problem. */
        boolean mapAlreadyRun = false;
        /** Manages the ZooKeeper servers if necessary (dynamic startup) */
        private ZooKeeperManager manager;
        /** Configuration */
        private Configuration conf;
        /** Already complete? */
        private boolean done = false;
        /** What kind of functions is this mapper doing? */
        private MapFunctions mapFunctions = MapFunctions.UNKNOWN;

        /** What kinds of functions to run on this mapper */
        public enum MapFunctions {
            UNKNOWN,
            MASTER_ONLY,
            MASTER_ZOOKEEPER_ONLY,
            WORKER_ONLY,
            ALL
        }

        /**
         * Get the map function enum
         */
        public MapFunctions getMapFunctions() {
            return mapFunctions;
        }

        /**
         * Get the worker communications, a subset of the functionality.
         *
         * @return worker communication object
         */
        public final WorkerCommunications<I, V, E, M>
                getWorkerCommunications() {
            return commService;
        }

        /**
         * Get the aggregator usage, a subset of the functionality
         *
         * @return
         */
        public final AggregatorUsage getAggregatorUsage() {
            return serviceWorker;
        }

        /**
         * Default handler for uncaught exceptions.
         */
        class OverrideExceptionHandler
                implements Thread.UncaughtExceptionHandler {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.fatal(
                    "uncaughtException: OverrideExceptionHandler on thread " +
                    t.getName() + ", msg = " +  e.getMessage() +
                    ", exiting...", e);
                System.exit(1);
            }
        }

        /**
         * Copied from JobConf to get the location of this jar.  Workaround for
         * things like Oozie map-reduce jobs.
         *
         * @param my_class Class to search the class loader path for to locate
         *        the relevant jar file
         * @return Location of the jar file containing my_class
         */
        private static String findContainingJar(Class<?> my_class) {
            ClassLoader loader = my_class.getClassLoader();
            String class_file =
                my_class.getName().replaceAll("\\.", "/") + ".class";
            try {
                for(Enumeration<?> itr = loader.getResources(class_file);
                itr.hasMoreElements();) {
                    URL url = (URL) itr.nextElement();
                    if ("jar".equals(url.getProtocol())) {
                        String toReturn = url.getPath();
                        if (toReturn.startsWith("file:")) {
                            toReturn = toReturn.substring("file:".length());
                        }
                        toReturn = URLDecoder.decode(toReturn, "UTF-8");
                        return toReturn.replaceAll("!.*$", "");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        /**
         * Make sure that all registered classes have matching types.  This
         * is a little tricky due to type erasure, cannot simply get them from
         * the class type arguments.  Also, set the vertex index, vertex value,
         * edge value and message value classes.
         *
         * @param conf Configuration to get the various classes
         */
        public void checkClassTypes(Configuration conf) {
            Class<? extends Vertex<I, V, E, M>> vertexClass =
                BspUtils.<I, V, E, M>getVertexClass(conf);
            List<Class<?>> classList = ReflectionUtils.<Vertex>getTypeArguments(
                Vertex.class, vertexClass);
            Type vertexIndexType = classList.get(0);
            Type vertexValueType = classList.get(1);
            Type edgeValueType = classList.get(2);
            Type messageValueType = classList.get(3);

            Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
                BspUtils.<I, V, E>getVertexInputFormatClass(conf);
            classList = ReflectionUtils.<VertexInputFormat>getTypeArguments(
                VertexInputFormat.class, vertexInputFormatClass);
            if (!vertexIndexType.equals(classList.get(0))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex index types don't match, " +
                    "vertex - " + vertexIndexType +
                    ", vertex input format - " + classList.get(0));
            }
            if (!vertexValueType.equals(classList.get(1))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex value types don't match, " +
                    "vertex - " + vertexValueType +
                    ", vertex input format - " + classList.get(1));
            }
            if (!edgeValueType.equals(classList.get(2))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Edge value types don't match, " +
                    "vertex - " + edgeValueType +
                    ", vertex input format - " + classList.get(2));
            }
            // If has vertex combiner class, check
            Class<? extends VertexCombiner<I, M>> vertexCombinerClass =
                BspUtils.<I, M>getVertexCombinerClass(conf);
            if (vertexCombinerClass != null) {
                classList = ReflectionUtils.<VertexCombiner>getTypeArguments(
                    VertexCombiner.class, vertexCombinerClass);
                if (!vertexIndexType.equals(classList.get(0))) {
                    throw new IllegalArgumentException(
                        "checkClassTypes: Vertex index types don't match, " +
                        "vertex - " + vertexIndexType +
                        ", vertex combiner - " + classList.get(0));
                }
                if (!messageValueType.equals(classList.get(1))) {
                    throw new IllegalArgumentException(
                        "checkClassTypes: Message value types don't match, " +
                        "vertex - " + vertexValueType +
                        ", vertex combiner - " + classList.get(1));
                }
            }
            // If has vertex output format class, check
            Class<? extends VertexOutputFormat<I, V, E>>
                vertexOutputFormatClass =
                    BspUtils.<I, V, E>getVertexOutputFormatClass(conf);
            if (vertexOutputFormatClass != null) {
                classList =
                    ReflectionUtils.<VertexOutputFormat>getTypeArguments(
                        VertexOutputFormat.class, vertexOutputFormatClass);
                if (!vertexIndexType.equals(classList.get(0))) {
                    throw new IllegalArgumentException(
                        "checkClassTypes: Vertex index types don't match, " +
                        "vertex - " + vertexIndexType +
                        ", vertex output format - " + classList.get(0));
                }
                if (!vertexValueType.equals(classList.get(1))) {
                    throw new IllegalArgumentException(
                        "checkClassTypes: Vertex value types don't match, " +
                        "vertex - " + vertexValueType +
                        ", vertex output format - " + classList.get(1));
                }
                if (!edgeValueType.equals(classList.get(2))) {
                    throw new IllegalArgumentException(
                        "checkClassTypes: Edge value types don't match, " +
                        "vertex - " + vertexIndexType +
                        ", vertex output format - " + classList.get(2));
                }
            }
            // Vertex range balancer might never select the types
            Class<? extends VertexRangeBalancer<I, V, E, M>>
                vertexRangeBalancerClass =
                    BspUtils.<I, V, E, M>getVertexRangeBalancerClass(conf);
            classList = ReflectionUtils.<VertexRangeBalancer>getTypeArguments(
                VertexRangeBalancer.class, vertexRangeBalancerClass);
            if (classList.get(0) != null &&
                    !vertexIndexType.equals(classList.get(0))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex index types don't match, " +
                    "vertex - " + vertexIndexType +
                    ", vertex range balancer - " + classList.get(0));
            }
            if (classList.get(1) != null &&
                    !vertexValueType.equals(classList.get(1))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex value types don't match, " +
                    "vertex - " + vertexValueType +
                    ", vertex range balancer - " + classList.get(1));
            }
            if (classList.get(2) != null &&
                    !edgeValueType.equals(classList.get(2))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Edge value types don't match, " +
                    "vertex - " + edgeValueType +
                    ", vertex range balancer - " + classList.get(2));
            }
            if (classList.get(3) != null &&
                    !messageValueType.equals(classList.get(3))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Message value types don't match, " +
                    "vertex - " + edgeValueType +
                    ", vertex range balancer - " + classList.get(3));
            }
            // Vertex resolver might never select the types
            Class<? extends VertexResolver<I, V, E, M>>
                vertexResolverClass =
                    BspUtils.<I, V, E, M>getVertexResolverClass(conf);
            classList = ReflectionUtils.<VertexResolver>getTypeArguments(
                VertexResolver.class, vertexResolverClass);
            if (classList.get(0) != null &&
                    !vertexIndexType.equals(classList.get(0))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex index types don't match, " +
                    "vertex - " + vertexIndexType +
                    ", vertex resolver - " + classList.get(0));
            }
            if (classList.get(1) != null &&
                    !vertexValueType.equals(classList.get(1))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex value types don't match, " +
                    "vertex - " + vertexValueType +
                    ", vertex resolver - " + classList.get(1));
            }
            if (classList.get(2) != null &&
                    !edgeValueType.equals(classList.get(2))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Edge value types don't match, " +
                    "vertex - " + edgeValueType +
                    ", vertex resolver - " + classList.get(2));
            }
            if (classList.get(3) != null &&
                    !messageValueType.equals(classList.get(3))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Message value types don't match, " +
                    "vertex - " + edgeValueType +
                    ", vertex resolver - " + classList.get(3));
            }
            conf.setClass(GiraphJob.VERTEX_INDEX_CLASS,
                          (Class<?>) vertexIndexType,
                          WritableComparable.class);
            conf.setClass(GiraphJob.VERTEX_VALUE_CLASS,
                          (Class<?>) vertexValueType,
                          Writable.class);
            conf.setClass(GiraphJob.EDGE_VALUE_CLASS,
                          (Class<?>) edgeValueType,
                          Writable.class);
            conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS,
                          (Class<?>) messageValueType,
                          Writable.class);
        }

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            // Setting the default handler for uncaught exceptions.
            Thread.setDefaultUncaughtExceptionHandler(
                new OverrideExceptionHandler());
            conf = context.getConfiguration();
            // Hadoop security needs this property to be set
            if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
                conf.set("mapreduce.job.credentials.binary",
                        System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            }
            // Ensure the user classes have matching types and figure them out
            checkClassTypes(conf);
            Vertex.setBspMapper(this);
            Vertex.setContext(context);

            // Do some initial setup (possibly starting up a Zookeeper service),
            // but mainly decide whether to load data
            // from a checkpoint or from the InputFormat.
            String jarFile = context.getJar();
            if (jarFile == null) {
                jarFile = findContainingJar(getClass());
            }
            String trimmedJarFile = jarFile.replaceFirst("file:", "");
            if (LOG.isInfoEnabled()) {
                LOG.info("setup: jar file @ " + jarFile +
                         ", using " + trimmedJarFile);
            }
            conf.set(ZOOKEEPER_JAR, trimmedJarFile);
            String serverPortList =
                conf.get(GiraphJob.ZOOKEEPER_LIST, "");
            if (serverPortList == "") {
                manager = new ZooKeeperManager(context);
                manager.setup();
                if (manager.computationDone()) {
                    done = true;
                    return;
                }
                manager.onlineZooKeeperServers();
                serverPortList = manager.getZooKeeperServerPortString();
            }
            context.setStatus("setup: Connected to Zookeeper service " +
                              serverPortList);

            if (conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
                        GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT) > 1) {
                Thread.sleep(GiraphJob.DEFAULT_ZOOKEEPER_INIT_LIMIT *
                             GiraphJob.DEFAULT_ZOOKEEPER_TICK_TIME);
            }
            int sessionMsecTimeout =
                conf.getInt(GiraphJob.ZOOKEEPER_SESSION_TIMEOUT,
                              GiraphJob.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
            boolean splitMasterWorker =
                conf.getBoolean(GiraphJob.SPLIT_MASTER_WORKER,
                                  GiraphJob.SPLIT_MASTER_WORKER_DEFAULT);
            int taskPartition = conf.getInt("mapred.task.partition", -1);

            // What functions should this mapper do?
            if (!splitMasterWorker) {
                mapFunctions = MapFunctions.ALL;
            }
            else {
                if (serverPortList != "") {
                    int masterCount =
                        conf.getInt(
                            GiraphJob.ZOOKEEPER_SERVER_COUNT,
                            GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
                    if (taskPartition < masterCount) {
                        mapFunctions = MapFunctions.MASTER_ONLY;
                    }
                    else {
                        mapFunctions = MapFunctions.WORKER_ONLY;
                    }
                }
                else {
                    if (manager.runsZooKeeper()) {
                        mapFunctions = MapFunctions.MASTER_ZOOKEEPER_ONLY;
                    }
                    else {
                        mapFunctions = MapFunctions.WORKER_ONLY;
                    }
                }
            }
            try {
                if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
                        (mapFunctions == MapFunctions.MASTER_ONLY) ||
                        (mapFunctions == MapFunctions.ALL)) {
                    LOG.info("setup: Starting up BspServiceMaster " +
                             "(master thread)...");
                    masterThread =
                        new MasterThread<I, V, E, M>(
                            new BspServiceMaster<I, V, E, M>(serverPortList,
                                                             sessionMsecTimeout,
                                                             context,
                                                             this));
                    masterThread.start();
                }
                if ((mapFunctions == MapFunctions.WORKER_ONLY) ||
                        (mapFunctions == MapFunctions.ALL)) {
                    LOG.info("setup: Starting up BspServiceWorker...");
                    serviceWorker = new BspServiceWorker<I, V, E, M>(
                        serverPortList, sessionMsecTimeout, context, this);
                    LOG.info("setup: Registering health of this worker...");
                    serviceWorker.setup();
                }
            } catch (Exception e) {
                LOG.error("setup: Caught exception just before end of setup", e);
                if (manager != null ) {
                    manager.offlineZooKeeperServers(
                    ZooKeeperManager.State.FAILED);
                }
                throw new RuntimeException(
                    "setup: Offlining servers due to exception...");
            }

            context.setStatus(getMapFunctions().toString() + " starting...");
        }

        @Override
        public void map(Object key, Object value, Context context)
            throws IOException, InterruptedException {
            // map() only does computation
            // 1) Run checkpoint per frequency policy.
            // 2) For every vertex on this mapper, run the compute() function
            // 3) Wait until all messaging is done.
            // 4) Check if all vertices are done.  If not goto 2).
            // 5) Dump output.
            if (done == true) {
                return;
            }
            if ((serviceWorker != null) &&
                    (serviceWorker.getTotalVertices() == 0)) {
                return;
            }

            if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
                    (mapFunctions == MapFunctions.MASTER_ONLY)) {
                LOG.info("map: No need to do anything when not a worker");
                return;
            }

            if (mapAlreadyRun) {
                throw new RuntimeException("In BSP, map should have only been" +
                                           " run exactly once, (already run)");
            }
            mapAlreadyRun = true;

            try {
                serviceWorker.getRepresentativeVertex().preApplication();
            } catch (InstantiationException e) {
                LOG.fatal("map: preApplication failed in instantiation", e);
                throw new RuntimeException(
                    "map: preApplication failed in instantiation", e);
            } catch (IllegalAccessException e) {
                LOG.fatal("map: preApplication failed in access", e);
                throw new RuntimeException(
                    "map: preApplication failed in access",e );
            }
            context.progress();

            long verticesFinished = 0;
            long edges = 0;
            Map<I, long []> maxIndexStatsMap = new TreeMap<I, long []>();
            do {
                long superstep = serviceWorker.getSuperstep();

                if (commService != null) {
                    commService.prepareSuperstep();
                }
                serviceWorker.startSuperstep();
                if (manager != null && manager.runsZooKeeper()) {
                    context.setStatus("Running Zookeeper Server");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("map: totalMem=" +
                              Runtime.getRuntime().totalMemory() +
                              " maxMem=" + Runtime.getRuntime().maxMemory() +
                              " freeMem=" + Runtime.getRuntime().freeMemory());
                }
                if ((superstep >= 1) && (commService == null)) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("map: Starting communication service...");
                    }
                    commService =
                        new RPCCommunications<I, V, E, M>(context,
                                                          serviceWorker);
                }
                context.progress();

                // Might need to restart from another superstep
                // (manually or automatic), or store a checkpoint
                if (serviceWorker.getRestartedSuperstep() == superstep) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("map: Loading from checkpoint " + superstep);
                    }
                    serviceWorker.loadCheckpoint(
                        serviceWorker.getRestartedSuperstep());
                } else if (serviceWorker.checkpointFrequencyMet(superstep)) {
                    serviceWorker.storeCheckpoint();
                }

                serviceWorker.exchangeVertexRanges();
                context.progress();

                maxIndexStatsMap.clear();
                Vertex.setSuperstep(superstep);
                Vertex.setNumVertices(serviceWorker.getTotalVertices());
                Vertex.setNumEdges(serviceWorker.getTotalEdges());

                serviceWorker.getRepresentativeVertex().preSuperstep();
                context.progress();

                for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
                    serviceWorker.getVertexRangeMap().entrySet()) {
                    // Only report my own vertex range stats
                    if (!entry.getValue().getHostname().equals(
                            serviceWorker.getHostname()) ||
                            (entry.getValue().getPort() !=
                            serviceWorker.getPort())) {
                        continue;
                    }

                    edges = 0;
                    verticesFinished = 0;
                    for (BasicVertex<I, V, E, M> vertex :
                            entry.getValue().getVertexMap().values()) {
                        if (!vertex.isHalted()) {
                            Iterator<M> vertexMsgIt =
                                vertex.getMsgList().iterator();
                            context.progress();
                            vertex.compute(vertexMsgIt);
                        }
                        if (vertex.isHalted()) {
                            ++verticesFinished;
                        }
                        edges += vertex.getOutEdgeMap().size();
                    }
                    long [] statArray = new long [3];
                    statArray[0] = verticesFinished;
                    statArray[1] = entry.getValue().getVertexMap().size();
                    statArray[2] = edges;
                    maxIndexStatsMap.put(entry.getKey(),
                                         statArray);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("map: " + statArray[0] + " of " +
                                 statArray[1] +
                                 " vertices finished for vertex range max " +
                                 "index = " + entry.getKey() +
                                 ", finished superstep " +
                                 serviceWorker.getSuperstep());
                    }
                }

                serviceWorker.getRepresentativeVertex().postSuperstep();
                context.progress();
                if (LOG.isInfoEnabled()) {
                    LOG.info("map: totalMem="
                             + Runtime.getRuntime().totalMemory() +
                             " maxMem=" + Runtime.getRuntime().maxMemory() +
                             " freeMem=" + Runtime.getRuntime().freeMemory());
                }
                commService.flush(context);
            } while (!serviceWorker.finishSuperstep(maxIndexStatsMap));

            if (LOG.isInfoEnabled()) {
                LOG.info("map: BSP application done " +
                         "(global vertices marked done)");
            }

            serviceWorker.getRepresentativeVertex().postApplication();
            context.progress();
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanup: Client done.");
            }
            if (done) {
                return;
            }

            if (commService != null) {
                commService.closeConnections();
            }
            if (serviceWorker != null) {
                serviceWorker.cleanup();
            }
            try {
                if (masterThread != null) {
                    masterThread.join();
                }
            } catch (InterruptedException e) {
                // cleanup phase -- just log the error
                LOG.error("cleanup: Master thread couldn't join");
            }
            if (manager != null) {
                manager.offlineZooKeeperServers(
                    ZooKeeperManager.State.FINISHED);
            }
            // Preferably would shut down the service only after
            // all clients have disconnected (or the exceptions on the
            // client side ignored).
            if (commService != null) {
                commService.close();
            }
        }
    }

    /**
     * Set the vertex class (required)
     *
     * @param vertexClass Runs vertex computation
     */
    final public void setVertexClass(Class<?> vertexClass) {
        getConfiguration().setClass(VERTEX_CLASS, vertexClass, Vertex.class);
    }

    /**
     * Set the vertex input format class (required)
     *
     * @param vertexInputFormatClass Determines how graph is input
     */
    final public void setVertexInputFormatClass(
            Class<?> vertexInputFormatClass) {
        getConfiguration().setClass(VERTEX_INPUT_FORMAT_CLASS,
                                    vertexInputFormatClass,
                                    VertexInputFormat.class);
    }

    /**
     * Set the vertex output format class (optional)
     *
     * @param vertexOutputFormatClass Determines how graph is output
     */
    final public void setVertexOutputFormatClass(
            Class<?> vertexOutputFormatClass) {
        getConfiguration().setClass(VERTEX_OUTPUT_FORMAT_CLASS,
                                    vertexOutputFormatClass,
                                    VertexOutputFormat.class);
    }

    /**
     * Set the vertex combiner class (optional)
     *
     * @param vertexCombinerClass Determines how vertex messages are combined
     */
    final public void setVertexCombinerClass(Class<?> vertexCombinerClass) {
        getConfiguration().setClass(VERTEX_COMBINER_CLASS,
                                    vertexCombinerClass,
                                    VertexCombiner.class);
    }

    /**
     * Set the vertex range balancer class (optional)
     *
     * @param vertexRangeBalancerClass Determines how vertex
     *        ranges are balanced prior to each superstep
     */
    final public void setVertexRangeBalancerClass(
            Class<?> vertexRangeBalancerClass) {
        getConfiguration().setClass(VERTEX_RANGE_BALANCER_CLASS,
                                    vertexRangeBalancerClass,
                                    VertexRangeBalancer.class);
    }

    /**
     * Set the vertex resolver class (optional)
     *
     * @param vertexResolverClass Determines how vertex mutations are resolved
     */
    final public void setVertexResolverClass(Class<?> vertexResolverClass) {
        getConfiguration().setClass(VERTEX_RESOLVER_CLASS,
                                    vertexResolverClass,
                                    VertexResolver.class);
    }

    /**
     * Set worker configuration for determining what is required for
     * a superstep.
     *
     * @param minWorkers Minimum workers to do a superstep
     * @param maxWorkers Maximum workers to do a superstep
     *        (max map tasks in job)
     * @param minPercentResponded 0 - 100 % of the workers required to
     *        have responded before continuing the superstep
     */
    final public void setWorkerConfiguration(int minWorkers,
                                             int maxWorkers,
                                             float minPercentResponded) {
        conf.setInt(MIN_WORKERS, minWorkers);
        conf.setInt(MAX_WORKERS, maxWorkers);
        conf.setFloat(MIN_PERCENT_RESPONDED, minPercentResponded);
    }

    /**
     * Utilize an existing ZooKeeper service.  If this is not set, ZooKeeper
     * will be dynamically started by Giraph for this job.
     *
     * @param serverList Comma separated list of servers and ports
     *        (i.e. zk1:2221,zk2:2221)
     */
    final public void setZooKeeperConfiguration(String serverList) {
        conf.set(ZOOKEEPER_LIST, serverList);
    }

    /**
     * Runs the actual graph application through Hadoop Map-Reduce.
     *
     * @param verbose If true, provide verbose output, false otherwise
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    final public boolean run(boolean verbose)
            throws IOException, InterruptedException, ClassNotFoundException {
        checkConfiguration();
        setNumReduceTasks(0);
        if (getJar() == null) {
            setJarByClass(GiraphJob.class);
        }
        setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
        setOutputFormatClass(BspOutputFormat.class);
        return waitForCompletion(verbose);
    }
}
