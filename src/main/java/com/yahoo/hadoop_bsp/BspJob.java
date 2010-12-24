package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 *
 * @author aching
 * @param <E>
 * @param <M>
 * @param <V>
 */
public class BspJob<V, E, M> extends Job {
    /** Minimum number of simultaneous processes before this job can run (int) */
    public static final String BSP_MIN_PROCESSES = "bsp.minProcs";
    /** Initial number of simultaneous tasks started by this job (int) */
    public static final String BSP_INITIAL_PROCESSES = "bsp.maxProcs";
    /** Minimum percent of initial processes that have responded (float) */
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
    /** Number of nodes to run Zookeeper on */
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
    public static int BSP_RPC_DEFAULT_HANDLERS = 20;
    /** Maximum number of messages per peer before flush */
    public static final String BSP_MSG_SIZE = "bsp.msgSize";
    /** Default maximum number of messages per peer before flush */
    public static int BSP_MSG_DEFAULT_SIZE = 1000;
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

    /**
     * If BSP_ZOOKEEPER_LIST is not set, then use this directory to manage
     * ZooKeeper
     */
    public static final String BSP_ZOOKEEPER_MANAGER_DIRECTORY =
        "bsp.zkManagerDirectory";
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
     * Default local ZooKeeper directory to use (where ZooKeeper server
     * files will go)
     */
    public static final String DEFAULT_BSP_ZOOKEEPER_DIR = "/tmp/bspZooKeeper";
    /**
     * Default ZooKeeper manager directory (where determining the servers in
     * HDFS files will go)
     */
    public static final String DEFAULT_ZOOKEEPER_MANAGER_DIRECTORY =
        "/tmp/_defaultZkManagerDir";
    /** Default poll attempts */
    public static final int DEFAULT_BSP_POLL_ATTEMPTS = 5;
    /** Default number of minimum vertices in each vertex range */
    public static final long DEFAULT_BSP_MIN_VERTICES_PER_RANGE = 3;
    /** Default total input split multiplier */
    public static final float DEFAULT_BSP_TOTAL_INPUT_SPLIT_MULTIPLIER = 0.5f;
    /** Default java opts passed to ZooKeeper startup */
    public static final String DEFAULT_BSP_ZOOKEEPER_JAVA_OPTS =
        "-Xmx512m";
    /**
     *  Constructor.
     * @param conf user-defined configuration
     * @param jobName user-defined job name
     * @throws IOException
     */
    public BspJob(
        Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        if (conf.getInt(BSP_INITIAL_PROCESSES, -1) < 0) {
            throw new IOException("No valid " + BSP_INITIAL_PROCESSES);
        }
        if (conf.getFloat(BSP_MIN_PERCENT_RESPONDED, 0.0f) <= 0) {
            throw new IOException("No valid " + BSP_MIN_PERCENT_RESPONDED);
        }
        if (conf.getInt(BSP_MIN_PROCESSES, -1) < 0) {
            throw new IOException("No valid " + BSP_MIN_PROCESSES);
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
    public static class BspMapper<I
        extends WritableComparable, V, E, M extends Writable>
        extends Mapper<Object, Object, Object, Object> {
        /** Logger */
        private static final Logger LOG = Logger.getLogger(BspMapper.class);
        /** Coordination service worker */
        CentralizedServiceWorker<I, V, E, M> m_serviceWorker;
        /** Coordination service master thread */
        Thread m_masterThread = null;
        /** Communication service */
        private RPCCommunications<I, V, E, M> m_commService;
        /** The map should be run exactly once, or else there is a problem. */
        boolean m_mapAlreadyRun = false;
        /** Manages the ZooKeeper servers if necessary (dynamic startup) */
        ZooKeeperManager m_manager;
        /** Configuration */
        Configuration m_conf = null;
        /** Already complete? */
        boolean m_done = false;

        /**
         * Passes message on to communication service.
         *
         * @param indx
         * @param msg
         */
        public void sendMsg(I indx, M msg) {
            m_commService.sendMessage(indx, msg);
        }

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {
            m_conf = context.getConfiguration();
            /*
             * Do some initial setup (possibly starting up a Zookeeper service),
             * but mainly decide whether to load data
             * from a checkpoint or from the InputFormat.
             */
            String jarFile = context.getJar();
            String trimmedJarFile = jarFile.replaceFirst("file:", "");
            LOG.info("setup: jar file @ " + jarFile +
                     ", using " + trimmedJarFile);
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
            int sessionMsecTimeout =
                m_conf.getInt(
                              BspJob.BSP_ZOOKEEPER_SESSION_TIMEOUT,
                              BspJob.DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT);
            try {
                LOG.info("Starting up BspServiceMaster (master thread)...");
                m_masterThread =
                    new MasterThread<I, V, E, M>(
                        new BspServiceMaster<I, V, E, M>(serverPortList,
                                                         sessionMsecTimeout,
                                                         context,
                                                         this));
                m_masterThread.start();
                LOG.info("Starting up BspServiceWorker...");
                m_serviceWorker = new BspServiceWorker<I, V, E, M>(
                        serverPortList, sessionMsecTimeout, context, this);
                LOG.info("Registering health of this process...");
                m_serviceWorker.setup();
            } catch (Exception e) {
                LOG.error(e.getMessage());
                if (m_manager != null ) {
                    m_manager.offlineZooKeeperServers();
                }
                throw new RuntimeException(e);
            }
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

            if (m_mapAlreadyRun) {
                throw new RuntimeException("In BSP, map should have only been" +
                                           " run exactly once, (already run)");
            }
            m_mapAlreadyRun = true;
            long verticesFinished = 0;
            Map<I, long []> maxIndexStatsMap = new TreeMap<I, long []>();
            do {
                 m_serviceWorker.startSuperstep();

                 long superStep = m_serviceWorker.getSuperstep();
                 LOG.info("map: superstep = " + superStep);
                 LOG.info("map: totalMem=" + Runtime.getRuntime().totalMemory() +
                          " maxMem=" + Runtime.getRuntime().maxMemory() +
                          " freeMem=" + Runtime.getRuntime().freeMemory());
                 if (m_serviceWorker.getSuperstep() == 1) {
                     LOG.info("map: Starting communication service...");
                     m_commService = new RPCCommunications<I, V, E, M>(
                         context, m_serviceWorker);
                 } else {
                     m_commService.prepareSuperstep();
                 }
                 context.progress();

                maxIndexStatsMap.clear();
                HadoopVertex.setSuperstep(superStep);
                HadoopVertex.setNumVertices(m_serviceWorker.getTotalVertices());
                Map<I, List<Vertex<I, V, E, M>>> maxIndexVertexListMap =
                    m_serviceWorker.getMaxIndexVertexLists();
                for (Map.Entry<I, List<Vertex<I, V, E, M>>> entry :
                    maxIndexVertexListMap.entrySet()) {
                    verticesFinished = 0;
                    for (Vertex<I, V, E, M> vertex : entry.getValue()) {
                        if (!vertex.isHalted()) {
                        Iterator<M> vertexMsgIt =
                            m_commService.getVertexMessageIterator(
                                vertex.getVertexId());
                            context.progress();
                            vertex.compute(vertexMsgIt);
                        }
                        if (vertex.isHalted()) {
                            ++verticesFinished;
                        }
                    }
                    long [] statArray = new long [2];
                    statArray[0] = verticesFinished;
                    statArray[1] = entry.getValue().size();
                    maxIndexStatsMap.put(entry.getKey(), statArray);
                    LOG.info("map: " + statArray[0] + " of " + statArray[1] +
                             " vertices for vertex range max index = " +
                             entry.getKey() +
                             ", finished superstep " +
                             m_serviceWorker.getSuperstep());
                }

                context.progress();
                LOG.info("map: totalMem=" + Runtime.getRuntime().totalMemory() +
                         " maxMem=" + Runtime.getRuntime().maxMemory() +
                         " freeMem=" + Runtime.getRuntime().freeMemory());
                m_commService.flush(context);
            } while (!m_serviceWorker.finishSuperstep(maxIndexStatsMap));

            LOG.info("map: BSP application done (global vertices marked done)");

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
            m_serviceWorker.cleanup();
            try {
                m_masterThread.join();
            } catch (InterruptedException e) {
                // cleanup phase -- just log the error
                LOG.error("cleanup: Master thread couldn't join");
            }
            if (m_manager != null) {
                m_manager.offlineZooKeeperServers();
            }
            // preferably would shut down the service only after
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
    public boolean run() throws IOException, InterruptedException,
        ClassNotFoundException {
        setNumReduceTasks(0);
        setJarByClass(BspJob.class);
        setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
        return waitForCompletion(true);
    }
}
