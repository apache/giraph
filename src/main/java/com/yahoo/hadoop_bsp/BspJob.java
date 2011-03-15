package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, our job should not have any reduce tasks.
 */
public class BspJob extends Job {
    /** Vertex class - required */
    public static final String BSP_VERTEX_CLASS = "bsp.vertexClass";
    /** InputSplit class - required */
    public static final String BSP_INPUT_SPLIT_CLASS = "bsp.inputSplitClass";
    /** InputFormat class - required */
    public static final String BSP_VERTEX_INPUT_FORMAT_CLASS =
        "bsp.vertexInputFormatClass";

    /** Combiner class - optional */
    public static final String BSP_COMBINER_CLASS =
        "bsp.combinerClass";
    /** Vertex writer class - optional */
    public static final String BSP_VERTEX_WRITER_CLASS =
        "bsp.vertexWriterClass";
    /** Vertex range balancer class - optional */
    public static final String BSP_VERTEX_RANGE_BALANCER_CLASS =
        "bsp.vertexRangeBalancerClass";

    /**
     * Minimum number of simultaneous workers before this job can run (int)
     */
    public static final String BSP_MIN_WORKERS = "bsp.minWorkers";
    /**
     * Maximum number of simultaneous worker tasks started by this job (int).
     */
    public static final String BSP_MAX_WORKERS = "bsp.maxWorkers";
    /**
     * Separate the workers and the master tasks.  This is required
     * to support dynamic recovery. (boolean)
     */
    public static final String BSP_SPLIT_MASTER_WORKER =
        "bsp.SplitMasterWorker";
    /**
     * Minimum percent of the maximum number of workers that have responded
     * in order to continue progressing. (float)
     */
    public static final String BSP_MIN_PERCENT_RESPONDED =
        "bsp.minPercentResponded";
    /** Polling timeout to check on the number of responded tasks (int) */
    public static final String BSP_POLL_MSECS = "bsp.pollMsecs";
    /** ZooKeeper list (empty for start up ZooKeeper locally) */
    public static final String BSP_ZOOKEEPER_LIST = "bsp.zkList";
    /** ZooKeeper session millisecond timeout */
    public static final String BSP_ZOOKEEPER_SESSION_TIMEOUT =
        "bsp.zkSessionMsecTimeout";
    /** Polling interval to check for the final ZooKeeper server data */
    public static final String BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS =
        "bsp.zkServerlistPollMsecs";
    /** Number of nodes (not tasks) to run Zookeeper on */
    public static final String BSP_ZOOKEEPER_SERVER_COUNT =
        "bsp.zkServerCount";
    /** ZooKeeper port to use */
    public static final String BSP_ZOOKEEPER_SERVER_PORT =
        "bsp.zkServerPort";
    /** Location of the ZooKeeper jar - Used internally, not meant for users */
    public static final String BSP_ZOOKEEPER_JAR = "bsp.zkJar";
    /** Local ZooKeeper directory to use */
    public static final String BSP_ZOOKEEPER_DIR = "bsp.zkDir";
    /** Initial port to start using for the RPC communication */
    public static final String BSP_RPC_INITIAL_PORT = "bsp.rpcInitialPort";
    /** Default port to start using for the RPC communication */
    public static final int BSP_RPC_DEFAULT_PORT = 61000;
    /** Maximum number of RPC handlers */
    public static final String BSP_RPC_NUM_HANDLERS = "bsp.rpcNumHandlers";
    /** Default maximum number of RPC handlers */
    public static final int BSP_RPC_DEFAULT_HANDLERS = 20;
    /** Maximum number of messages per peer before flush */
    public static final String BSP_MSG_SIZE = "bsp.msgSize";
    /** Default maximum number of messages per peer before flush */
    public static final int BSP_MSG_DEFAULT_SIZE = 1000;
    /** Number of poll attempts prior to failing the job (int) */
    public static final String BSP_POLL_ATTEMPTS = "bsp.pollAttempts";
    /** Number of minimum vertices in each vertex range */
    public static final String BSP_MIN_VERTICES_PER_RANGE =
        "bsp.minVerticesPerRange";
    /**
     * Set the multiplicative factor of how many partitions to create from
     * a single InputSplit based on the number of total InputSplits.  For
     * example, if there are 10 total InputSplits and this is set to 0.5, then
     * you will get 0.5 * 10 = 5 partitions for every InputSplit (given that the
     * minimum size is met).
     */
    public static final String BSP_TOTAL_INPUT_SPLIT_MULTIPLIER =
        "bsp.totalInputSplitMultiplier";
    /** Java opts passed to ZooKeeper startup */
    public static final String BSP_ZOOKEEPER_JAVA_OPTS =
        "bsp.zkJavaOpts";
    /** How often to checkpoint (i.e. 1 means every superstep, 2 is every
     *  two supersteps, etc.).
     */
    public static final String BSP_CHECKPOINT_FREQUENCY =
        "bsp.checkpointFrequency";

    /**
     * An application can be restarted manually by selecting a superstep.  The
     * corresponding checkpoint must exist for this to work.  The user should
     * set a long value.
     */
    public static final String BSP_RESTART_SUPERSTEP = "bsp.restartSuperstep";

    /**
     * If BSP_ZOOKEEPER_LIST is not set, then use this directory to manage
     * ZooKeeper
     */
    public static final String BSP_ZOOKEEPER_MANAGER_DIRECTORY =
        "bsp.zkManagerDirectory";
    /** This directory has/stores the available checkpoint files in HDFS. */
    public static final String BSP_CHECKPOINT_DIRECTORY =
        "bsp.checkpointDirectory";
    /** Keep the zookeeper output for debugging? Default is to remove it. */
    public static final String BSP_KEEP_ZOOKEEPER_DATA =
        "bsp.keepZooKeeperData";

    /**
     * Default on whether to separate the workers and the master tasks.
     * This is required to support dynamic recovery.
     */
    public static final Boolean DEFAULT_BSP_SPLIT_MASTER_WORKER = true;
    /** Default poll msecs (30 seconds) */
    public static final int DEFAULT_BSP_POLL_MSECS = 30*1000;
    /** Default Zookeeper session millisecond timeout */
    public static final int DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT = 60*1000;
    /** Default polling interval to check for the final ZooKeeper server data */
    public static final int DEFAULT_BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS =
        3*1000;
    /** Default number of nodes to run Zookeeper on */
    public static final int DEFAULT_BSP_ZOOKEEPER_SERVER_COUNT = 1;
    /** Default ZooKeeper port to use */
    public static final int DEFAULT_BSP_ZOOKEEPER_SERVER_PORT = 22181;
    /** Default port to start using for the RPC communication */
    public static final int DEFAULT_BSP_RPC_INITIAL_PORT = 30000;
    /**
     * Default local ZooKeeper prefix directory to use (where ZooKeeper server
     * files will go)
     */
    public static final String DEFAULT_BSP_ZOOKEEPER_DIR = "/tmp/bspZooKeeper";
    /**
     * Default ZooKeeper manager directory (where determining the servers in
     * HDFS files will go).  Final directory path will also have job number
     * for uniqueness.
     */
    public static final String DEFAULT_ZOOKEEPER_MANAGER_DIR =
        "/tmp/_bsp/_defaultZkManagerDir";
    /**
     * Default checkpoint directory (where checkpoing files go in HDFS).  Final
     * directory path will also have the job number for uniqueness
     */
    public static final String DEFAULT_BSP_CHECKPOINT_DIRECTORY =
        "/tmp/_bsp/_checkpoints/";
    /** Default is to remove ZooKeeper data. */
    public static final Boolean DEFAULT_BSP_KEEP_ZOOKEEPER_DATA = false;
    /** Default poll attempts */
    public static final int DEFAULT_BSP_POLL_ATTEMPTS = 5;
    /** Default number of minimum vertices in each vertex range */
    public static final long DEFAULT_BSP_MIN_VERTICES_PER_RANGE = 3;
    /** Default total input split multiplier */
    public static final float DEFAULT_BSP_TOTAL_INPUT_SPLIT_MULTIPLIER = 0.5f;
    /** Default java opts passed to ZooKeeper startup */
    public static final String DEFAULT_BSP_ZOOKEEPER_JAVA_OPTS =
        "-Xmx128m";
    /** Default checkpointing frequency of every 2 supersteps. */
    public static final int DEFAULT_BSP_CHECKPOINT_FREQUENCY = 2;
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
    public BspJob(
        Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        if (conf.getInt(BSP_MAX_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + BSP_MAX_WORKERS);
        }
        if (conf.getFloat(BSP_MIN_PERCENT_RESPONDED, 0.0f) <= 0) {
            throw new RuntimeException("No valid " + BSP_MIN_PERCENT_RESPONDED);
        }
        if (conf.getInt(BSP_MIN_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + BSP_MIN_WORKERS);
        }
        if (conf.getClass(BSP_VERTEX_CLASS,
                          HadoopVertex.class,
                          HadoopVertex.class) == null) {
            throw new RuntimeException("BspJob: Null BSP_VERTEX_CLASS");
        }
        if (conf.getClass(BSP_INPUT_SPLIT_CLASS,
                          InputSplit.class,
                          InputSplit.class) == null) {
            throw new RuntimeException("BspJob: Null BSP_INPUT_SPLIT_CLASS");
        }
        if (conf.getClass(BSP_VERTEX_INPUT_FORMAT_CLASS,
                          VertexInputFormat.class,
                          VertexInputFormat.class) == null) {
            throw new RuntimeException(
                "BspJob: Null BSP_VERTEX_INPUT_FORMAT_CLASS");
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

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {

            // Setting the default handler for uncaught exceptions.
            Thread.setDefaultUncaughtExceptionHandler(
                new OverrideExceptionHandler());
            // Do some initial setup (possibly starting up a Zookeeper service),
            // but mainly decide whether to load data
            // from a checkpoint or from the InputFormat.
            String jarFile = context.getJar();
            String trimmedJarFile = jarFile.replaceFirst("file:", "");
            LOG.info("setup: jar file @ " + jarFile +
                     ", using " + trimmedJarFile);
            m_conf = context.getConfiguration();
            m_conf.set(BSP_ZOOKEEPER_JAR, trimmedJarFile);
            String serverPortList =
                m_conf.get(BspJob.BSP_ZOOKEEPER_LIST, "");
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
            if (m_conf.getInt(BspJob.BSP_ZOOKEEPER_SERVER_COUNT,
                        BspJob.DEFAULT_BSP_ZOOKEEPER_SERVER_COUNT) > 1) {
                Thread.sleep(BspJob.DEFAULT_ZOOKEEPER_INIT_LIMIT *
                             BspJob.DEFAULT_ZOOKEEPER_TICK_TIME);
            }
            int sessionMsecTimeout =
                m_conf.getInt(BspJob.BSP_ZOOKEEPER_SESSION_TIMEOUT,
                              BspJob.DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT);
            boolean splitMasterWorker =
                m_conf.getBoolean(BspJob.BSP_SPLIT_MASTER_WORKER,
                                  BspJob.DEFAULT_BSP_SPLIT_MASTER_WORKER);
            int taskPartition = m_conf.getInt("mapred.task.partition", -1);

            // What functions should this mapper do?
            if (!splitMasterWorker) {
                m_mapFunctions = MapFunctions.ALL;
            }
            else {
                if (serverPortList != "") {
                    int masterCount =
                        m_conf.getInt(
                            BspJob.BSP_ZOOKEEPER_SERVER_COUNT,
                            BspJob.DEFAULT_BSP_ZOOKEEPER_SERVER_COUNT);
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
                LOG.error(e.getMessage());
                if (m_manager != null ) {
                    m_manager.offlineZooKeeperServers(
                    ZooKeeperManager.State.FAILED);
                }
                throw new RuntimeException(e);
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

            m_serviceWorker.getRepresentativeVertex().preApplication();
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

                // Might need to restart from another superstep (manually), or
                // store a checkpoint
                if (m_serviceWorker.getManualRestartSuperstep() == superstep) {
                    m_serviceWorker.loadCheckpoint(
                        m_serviceWorker.getManualRestartSuperstep());
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
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    final public boolean run() throws IOException, InterruptedException,
        ClassNotFoundException {
        setNumReduceTasks(0);
        if (getJar() == null) {
            setJarByClass(BspJob.class);
        }
        setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
        return waitForCompletion(true);
    }
}
