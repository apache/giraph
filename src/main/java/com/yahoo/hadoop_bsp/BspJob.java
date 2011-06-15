package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
public class BspJob extends Job {
    /** Vertex class - required */
    public static final String VERTEX_CLASS = "giraph.vertexClass";
    /** InputFormat class - required */
    public static final String VERTEX_INPUT_FORMAT_CLASS =
        "giraph.vertexInputFormatClass";

    /** Combiner class - optional */
    public static final String COMBINER_CLASS =
        "giraph.combinerClass";
    /** Vertex writer class - optional */
    public static final String VERTEX_WRITER_CLASS =
        "giraph.vertexWriterClass";
    /** Vertex range balancer class - optional */
    public static final String VERTEX_RANGE_BALANCER_CLASS =
        "giraph.vertexRangeBalancerClass";

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
    /**
     * Default local ZooKeeper prefix directory to use (where ZooKeeper server
     * files will go)
     */
    public static String ZOOKEEPER_DIR_DEFAULT =
        System.getProperty("user.dir") + "/_bspZooKeeper";

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

    /**
     *  Constructor.
     * @param conf user-defined configuration
     * @param jobName user-defined job name
     * @throws IOException
     */
    public BspJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        if (conf.getInt(MAX_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + MAX_WORKERS);
        }
        if (conf.getFloat(MIN_PERCENT_RESPONDED,
                          MIN_PERCENT_RESPONDED_DEFAULT) <= 0.0f ||
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) > 100.0f) {
            throw new RuntimeException(
                "Invalid " +
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) + " for " +
                MIN_PERCENT_RESPONDED);
        }
        if (conf.getInt(MIN_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + MIN_WORKERS);
        }
        if (conf.getClass(VERTEX_CLASS,
                          HadoopVertex.class,
                          HadoopVertex.class) == null) {
            throw new RuntimeException("BspJob: Null VERTEX_CLASS");
        }
        if (conf.getClass(VERTEX_INPUT_FORMAT_CLASS,
                          VertexInputFormat.class,
                          VertexInputFormat.class) == null) {
            throw new RuntimeException(
                "BspJob: Null VERTEX_INPUT_FORMAT_CLASS");
        }
        String jobLocalDir = conf.get("job.local.dir");
        if (jobLocalDir != null) { // for non-local jobs
            ZOOKEEPER_DIR_DEFAULT = jobLocalDir +
                "/_bspZooKeeper";
        }
    }

    /**
     * The mapper that will execute the BSP tasks.  Since this mapper will
     * not be passing data by key-value pairs through the MR framework, the
     * types are irrelevant.
     *
     * @author aching
     * @param <V>
     * @param <V>
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
        CentralizedServiceWorker<I, V, E, M> m_serviceWorker;
        /** Coordination service master thread */
        Thread m_masterThread = null;
        /** Communication service */
        private ServerInterface<I, V, E, M> m_commService = null;
        /** The map should be run exactly once, or else there is a problem. */
        boolean m_mapAlreadyRun = false;
        /** Manages the ZooKeeper servers if necessary (dynamic startup) */
        private ZooKeeperManager m_manager;
        /** Configuration */
        private Configuration m_conf;
        /** Already complete? */
        private boolean m_done = false;
        /** What kind of functions is this mapper doing? */
        private MapFunctions m_mapFunctions = MapFunctions.UNKNOWN;

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
        MapFunctions getMapFunctions() {
            return m_mapFunctions;
        }

        /**
         * Get the worker communications, a subset of the functionality.
         *
         * @return worker communication object
         */
        public final WorkerCommunications<I, V, E, M>
                getWorkerCommunications() {
            return m_commService;
        }

        /**
         * Get the aggregator usage, a subset of the functionality
         *
         * @return
         */
        public final AggregatorUsage getAggregatorUsage() {
            return m_serviceWorker;
        }

        /**
         * Default handler for uncaught exceptions.
         *
         */
        class OverrideExceptionHandler
                implements Thread.UncaughtExceptionHandler {
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println(
                    "uncaughtException: OverrideExceptionHandler on thread " +
                    t.getName() + ", msg = " +  e.getMessage() +
                    ", exiting...");
                e.printStackTrace();
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
        private static String findContainingJar(Class my_class) {
            ClassLoader loader = my_class.getClassLoader();
            String class_file =
                my_class.getName().replaceAll("\\.", "/") + ".class";
            try {
                for(Enumeration itr = loader.getResources(class_file);
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

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {

            // Setting the default handler for uncaught exceptions.
            Thread.setDefaultUncaughtExceptionHandler(
                new OverrideExceptionHandler());
            // Hadoop security needs this property to be set
            m_conf = context.getConfiguration();
            if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
                m_conf.set("mapreduce.job.credentials.binary",
                        System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            }
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
            m_conf.set(ZOOKEEPER_JAR, trimmedJarFile);
            String serverPortList =
                m_conf.get(BspJob.ZOOKEEPER_LIST, "");
            if (serverPortList == "") {
                m_manager = new ZooKeeperManager(context);
                m_manager.setup();
                if (m_manager.computationDone()) {
                    m_done = true;
                    return;
                }
                m_manager.onlineZooKeeperServers();
                serverPortList = m_manager.getZooKeeperServerPortString();
            }
            context.setStatus("setup: Connected to Zookeeper service " +
                              serverPortList);

            if (m_conf.getInt(BspJob.ZOOKEEPER_SERVER_COUNT,
                        BspJob.ZOOKEEPER_SERVER_COUNT_DEFAULT) > 1) {
                Thread.sleep(BspJob.DEFAULT_ZOOKEEPER_INIT_LIMIT *
                             BspJob.DEFAULT_ZOOKEEPER_TICK_TIME);
            }
            int sessionMsecTimeout =
                m_conf.getInt(BspJob.ZOOKEEPER_SESSION_TIMEOUT,
                              BspJob.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
            boolean splitMasterWorker =
                m_conf.getBoolean(BspJob.SPLIT_MASTER_WORKER,
                                  BspJob.SPLIT_MASTER_WORKER_DEFAULT);
            int taskPartition = m_conf.getInt("mapred.task.partition", -1);

            // What functions should this mapper do?
            if (!splitMasterWorker) {
                m_mapFunctions = MapFunctions.ALL;
            }
            else {
                if (serverPortList != "") {
                    int masterCount =
                        m_conf.getInt(
                            BspJob.ZOOKEEPER_SERVER_COUNT,
                            BspJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
                    if (taskPartition < masterCount) {
                        m_mapFunctions = MapFunctions.MASTER_ONLY;
                    }
                    else {
                        m_mapFunctions = MapFunctions.WORKER_ONLY;
                    }
                }
                else {
                    if (m_manager.runsZooKeeper()) {
                        m_mapFunctions = MapFunctions.MASTER_ZOOKEEPER_ONLY;
                    }
                    else {
                        m_mapFunctions = MapFunctions.WORKER_ONLY;
                    }
                }
            }
            try {
                if ((m_mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
                        (m_mapFunctions == MapFunctions.MASTER_ONLY) ||
                        (m_mapFunctions == MapFunctions.ALL)) {
                    LOG.info("setup: Starting up BspServiceMaster " +
                             "(master thread)...");
                    m_masterThread =
                        new MasterThread<I, V, E, M>(
                            new BspServiceMaster<I, V, E, M>(serverPortList,
                                                             sessionMsecTimeout,
                                                             context,
                                                             this));
                    m_masterThread.start();
                }
                if ((m_mapFunctions == MapFunctions.WORKER_ONLY) ||
                        (m_mapFunctions == MapFunctions.ALL)) {
                    LOG.info("setup: Starting up BspServiceWorker...");
                    m_serviceWorker = new BspServiceWorker<I, V, E, M>(
                        serverPortList, sessionMsecTimeout, context, this);
                    LOG.info("setup: Registering health of this worker...");
                    m_serviceWorker.setup();
                }
            } catch (Exception e) {
                LOG.error("setup: Caught exception just before end of setup", e);
                if (m_manager != null ) {
                    m_manager.offlineZooKeeperServers(
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
            if (m_done == true) {
                return;
            }
            if ((m_serviceWorker != null) &&
                    (m_serviceWorker.getTotalVertices() == 0)) {
                return;
            }

            if ((m_mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
                    (m_mapFunctions == MapFunctions.MASTER_ONLY)) {
                LOG.info("map: No need to do anything when not a worker");
                return;
            }

            if (m_mapAlreadyRun) {
                throw new RuntimeException("In BSP, map should have only been" +
                                           " run exactly once, (already run)");
            }
            m_mapAlreadyRun = true;

            HadoopVertex.setContext(context);
            try {
                m_serviceWorker.getRepresentativeVertex().preApplication();
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
            Map<I, long []> maxIndexStatsMap = new TreeMap<I, long []>();
            do {
                long superstep = m_serviceWorker.getSuperstep();

                if (m_commService != null) {
                    m_commService.prepareSuperstep();
                }
                m_serviceWorker.startSuperstep();
                if (m_manager != null && m_manager.runsZooKeeper()) {
                    context.setStatus("Running Zookeeper Server");
                }

                LOG.info("map: superstep = " + superstep);
                LOG.debug("map: totalMem=" + Runtime.getRuntime().totalMemory() +
                          " maxMem=" + Runtime.getRuntime().maxMemory() +
                          " freeMem=" + Runtime.getRuntime().freeMemory());
                if ((superstep >= 1) && (m_commService == null)) {
                    LOG.info("map: Starting communication service...");
                    m_commService =
                        new RPCCommunications<I, V, E, M>(context,
                                                          m_serviceWorker);
                }
                context.progress();

                // Might need to restart from another superstep
                // (manually or automatic), or store a checkpoint
                if (m_serviceWorker.getRestartedSuperstep() == superstep) {
                    LOG.info("map: Loading from checkpoint " + superstep);
                    m_serviceWorker.loadCheckpoint(
                        m_serviceWorker.getRestartedSuperstep());
                } else if (m_serviceWorker.checkpointFrequencyMet(superstep)) {
                    m_serviceWorker.storeCheckpoint();
                }

                m_serviceWorker.exchangeVertexRanges();
                context.progress();

                maxIndexStatsMap.clear();
                HadoopVertex.setSuperstep(superstep);
                HadoopVertex.setNumVertices(m_serviceWorker.getTotalVertices());

                m_serviceWorker.getRepresentativeVertex().preSuperstep();
                context.progress();

                for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
                    m_serviceWorker.getVertexRangeMap().entrySet()) {
                    // Only report my own vertex range stats
                    if (!entry.getValue().getHostname().equals(
                            m_serviceWorker.getHostname()) ||
                            (entry.getValue().getPort() !=
                            m_serviceWorker.getPort())) {
                        continue;
                    }

                    verticesFinished = 0;
                    for (Vertex<I, V, E, M> vertex :
                            entry.getValue().getVertexList()) {
                        if (!vertex.isHalted()) {
                            Iterator<M> vertexMsgIt =
                                vertex.getMsgList().iterator();
                            context.progress();
                            vertex.compute(vertexMsgIt);
                        }
                        if (vertex.isHalted()) {
                            ++verticesFinished;
                        }
                    }
                    long [] statArray = new long [2];
                    statArray[0] = verticesFinished;
                    statArray[1] = entry.getValue().getVertexList().size();
                    maxIndexStatsMap.put(entry.getKey(),
                                         statArray);
                    LOG.info("map: " + statArray[0] + " of " + statArray[1] +
                             " vertices finished for vertex range max " +
                             "index = " + entry.getKey() +
                             ", finished superstep " +
                             m_serviceWorker.getSuperstep());
                }

                m_serviceWorker.getRepresentativeVertex().postSuperstep();
                context.progress();
                LOG.info("map: totalMem=" + Runtime.getRuntime().totalMemory() +
                         " maxMem=" + Runtime.getRuntime().maxMemory() +
                         " freeMem=" + Runtime.getRuntime().freeMemory());
                m_commService.flush(context);
            } while (!m_serviceWorker.finishSuperstep(maxIndexStatsMap));

            LOG.info("map: BSP application done (global vertices marked done)");

            m_serviceWorker.getRepresentativeVertex().postApplication();
            context.progress();
        }

        @Override
        public void cleanup(Context context)
            throws IOException, InterruptedException {
            LOG.info("cleanup: Client done.");
            if (m_done) {
                return;
            }

            if (m_commService != null) {
                m_commService.closeConnections();
            }
            if (m_serviceWorker != null) {
                m_serviceWorker.cleanup();
            }
            try {
                if (m_masterThread != null) {
                    m_masterThread.join();
                }
            } catch (InterruptedException e) {
                // cleanup phase -- just log the error
                LOG.error("cleanup: Master thread couldn't join");
            }
            if (m_manager != null) {
                m_manager.offlineZooKeeperServers(
                    ZooKeeperManager.State.FINISHED);
            }
            // Preferably would shut down the service only after
            // all clients have disconnected (or the exceptions on the
            // client side ignored).
            if (m_commService != null) {
                m_commService.close();
            }
        }
    }

    /**
     * Runs the actual BSPJob through Hadoop.
     *
     * @param verbose If true, provide verbose output, false otherwise
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    final public boolean run(boolean verbose)
            throws IOException, InterruptedException, ClassNotFoundException {
        setNumReduceTasks(0);
        if (getJar() == null) {
            setJarByClass(BspJob.class);
        }
        setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
        return waitForCompletion(verbose);
    }
}
