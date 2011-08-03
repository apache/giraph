package org.apache.giraph.graph;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.RPCCommunications;
import org.apache.giraph.comm.ServerInterface;
import org.apache.giraph.comm.WorkerCommunications;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * This mapper that will execute the BSP graph tasks.  Since this mapper will
 * not be passing data by key-value pairs through the MR framework, the
 * types are irrelevant.
 */
@SuppressWarnings("rawtypes")
public class GraphMapper<I extends WritableComparable, V extends Writable,
        E extends Writable, M extends Writable> extends
        Mapper<Object, Object, Object, Object> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GraphMapper.class);
    /** Coordination service worker */
    CentralizedServiceWorker<I, V, E, M> serviceWorker;
    /** Coordination service master thread */
    Thread masterThread = null;
    /** Communication service */
    private ServerInterface<I, V, E, M> commService = null;
    /** The map should be run exactly once, or else there is a problem. */
    boolean mapAlreadyRun = false;
    /** Manages the ZooKeeper servers if necessary (dynamic startup) */
    private ZooKeeperManager zkManager;
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
        ALL,
        ALL_EXCEPT_ZOOKEEPER
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
     * @return Aggregator usage interface
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
    public void determineClassTypes(Configuration conf) {
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
        if (classList.get(0) == null) {
            LOG.warn("Input format vertex index type is not known");
        } else if (!vertexIndexType.equals(classList.get(0))) {
            throw new IllegalArgumentException(
                "checkClassTypes: Vertex index types don't match, " +
                "vertex - " + vertexIndexType +
                ", vertex input format - " + classList.get(0));
        }
        if (classList.get(1) == null) {
            LOG.warn("Input format vertex value type is not known");
        } else if (!vertexValueType.equals(classList.get(1))) {
            throw new IllegalArgumentException(
                "checkClassTypes: Vertex value types don't match, " +
                "vertex - " + vertexValueType +
                ", vertex input format - " + classList.get(1));
        }
        if (classList.get(2) == null) {
            LOG.warn("Input format edge value type is not known");
        } else if (!edgeValueType.equals(classList.get(2))) {
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
            if (classList.get(0) == null) {
                LOG.warn("Output format vertex index type is not known");
            } else if (!vertexIndexType.equals(classList.get(0))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex index types don't match, " +
                    "vertex - " + vertexIndexType +
                    ", vertex output format - " + classList.get(0));
            }
            if (classList.get(1) == null) {
                LOG.warn("Output format vertex value type is not known");
            } else if (!vertexValueType.equals(classList.get(1))) {
                throw new IllegalArgumentException(
                    "checkClassTypes: Vertex value types don't match, " +
                    "vertex - " + vertexValueType +
                    ", vertex output format - " + classList.get(1));
            } if (classList.get(2) == null) {
                LOG.warn("Output format edge value type is not known");
            } else if (!edgeValueType.equals(classList.get(2))) {
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

    /**
     * Figure out what functions this mapper should do.  Basic logic is as
     * follows:
     * 1) If not split master, everyone does the everything and/or running
     *    ZooKeeper.
     * 2) If split master/worker, masters also run ZooKeeper (if it's not
     *    given to us).
     *
     * @param conf Configuration to use
     * @return Functions that this mapper should do.
     */
    private static MapFunctions determineMapFunctions(
            Configuration conf,
            ZooKeeperManager zkManager) {
        boolean splitMasterWorker =
            conf.getBoolean(GiraphJob.SPLIT_MASTER_WORKER,
                            GiraphJob.SPLIT_MASTER_WORKER_DEFAULT);
        int taskPartition = conf.getInt("mapred.task.partition", -1);
        boolean zkAlreadyProvided =
            conf.get(GiraphJob.ZOOKEEPER_LIST) != null;
        MapFunctions functions = MapFunctions.UNKNOWN;
        // What functions should this mapper do?
        if (!splitMasterWorker) {
            if ((zkManager != null) && zkManager.runsZooKeeper()) {
                functions = MapFunctions.ALL;
            } else {
                functions = MapFunctions.ALL_EXCEPT_ZOOKEEPER;
            }
        } else {
            if (zkAlreadyProvided) {
                int masterCount =
                    conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
                                GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
                if (taskPartition < masterCount) {
                    functions = MapFunctions.MASTER_ONLY;
                } else {
                    functions = MapFunctions.WORKER_ONLY;
                }
            } else {
                if ((zkManager != null) && zkManager.runsZooKeeper()) {
                    functions = MapFunctions.MASTER_ZOOKEEPER_ONLY;
                } else {
                    functions = MapFunctions.WORKER_ONLY;
                }
            }
        }
        return functions;
    }

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        context.setStatus("setup: Beginning mapper setup.");
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
        determineClassTypes(conf);
        Vertex.setGraphMapper(this);
        Vertex.setContext(context);

        // Do some initial setup (possibly starting up a Zookeeper service)
        context.setStatus("setup: Initializing Zookeeper services.");
        String jarFile = context.getJar();
        if (jarFile == null) {
            jarFile = findContainingJar(getClass());
        }
        String trimmedJarFile = jarFile.replaceFirst("file:", "");
        if (LOG.isInfoEnabled()) {
            LOG.info("setup: jar file @ " + jarFile +
                     ", using " + trimmedJarFile);
        }
        conf.set(GiraphJob.ZOOKEEPER_JAR, trimmedJarFile);
        String serverPortList =
            conf.get(GiraphJob.ZOOKEEPER_LIST, "");
        if (serverPortList == "") {
            zkManager = new ZooKeeperManager(context);
            context.setStatus("setup: Setting up Zookeeper manager.");
            zkManager.setup();
            if (zkManager.computationDone()) {
                done = true;
                return;
            }
            zkManager.onlineZooKeeperServers();
            serverPortList = zkManager.getZooKeeperServerPortString();
        }
        context.setStatus("setup: Connected to Zookeeper service " +
                          serverPortList);
        this.mapFunctions = determineMapFunctions(conf, zkManager);

        // Sometimes it takes a while to get multiple ZooKeeper servers up
        if (conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
                    GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT) > 1) {
            Thread.sleep(GiraphJob.DEFAULT_ZOOKEEPER_INIT_LIMIT *
                         GiraphJob.DEFAULT_ZOOKEEPER_TICK_TIME);
        }
        int sessionMsecTimeout =
            conf.getInt(GiraphJob.ZOOKEEPER_SESSION_TIMEOUT,
                          GiraphJob.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
        try {
            if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
                    (mapFunctions == MapFunctions.MASTER_ONLY) ||
                    (mapFunctions == MapFunctions.ALL) ||
                    (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("setup: Starting up BspServiceMaster " +
                             "(master thread)...");
                }
                masterThread =
                    new MasterThread<I, V, E, M>(
                        new BspServiceMaster<I, V, E, M>(serverPortList,
                                                         sessionMsecTimeout,
                                                         context,
                                                         this),
                                                         context);
                masterThread.start();
            }
            if ((mapFunctions == MapFunctions.WORKER_ONLY) ||
                    (mapFunctions == MapFunctions.ALL) ||
                    (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("setup: Starting up BspServiceWorker...");
                }
                serviceWorker = new BspServiceWorker<I, V, E, M>(
                    serverPortList, sessionMsecTimeout, context, this);
                if (LOG.isInfoEnabled()) {
                    LOG.info("setup: Registering health of this worker...");
                }
                serviceWorker.setup();
            }
        } catch (Exception e) {
            LOG.error("setup: Caught exception just before end of setup", e);
            if (zkManager != null ) {
                zkManager.offlineZooKeeperServers(
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
            if (LOG.isInfoEnabled()) {
                LOG.info("map: No need to do anything when not a worker");
            }
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

        long workerFinishedVertices = 0;
        long workerVertices = 0;
        long workerEdges = 0;
        long workerSentMessages = 0;
        do {
            long superstep = serviceWorker.getSuperstep();

            if (commService != null) {
                commService.prepareSuperstep();
            }
            serviceWorker.startSuperstep();
            if (zkManager != null && zkManager.runsZooKeeper()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("map: Chosen to run ZooKeeper...");
                }
                context.setStatus("map: Running Zookeeper Server");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("map: totalMem=" +
                          Runtime.getRuntime().totalMemory() +
                          " maxMem=" + Runtime.getRuntime().maxMemory() +
                          " freeMem=" + Runtime.getRuntime().freeMemory());
            }
            if ((superstep > BspService.INPUT_SUPERSTEP) &&
                    (commService == null)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("map: Starting communication service on " +
                             "superstep " + superstep);
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

            Vertex.setSuperstep(superstep);
            Vertex.setNumVertices(serviceWorker.getTotalVertices());
            Vertex.setNumEdges(serviceWorker.getTotalEdges());

            serviceWorker.getRepresentativeVertex().preSuperstep();
            context.progress();

            workerFinishedVertices = 0;
            workerVertices = 0;
            workerEdges = 0;
            workerSentMessages = 0;
            for (Map.Entry<I, VertexRange<I, V, E, M>> entry :
                serviceWorker.getVertexRangeMap().entrySet()) {
                // Only report my own vertex range stats
                if (!entry.getValue().getHostname().equals(
                        serviceWorker.getHostname()) ||
                        (entry.getValue().getPort() !=
                        serviceWorker.getPort())) {
                    continue;
                }

                for (BasicVertex<I, V, E, M> vertex :
                        entry.getValue().getVertexMap().values()) {
                    if (vertex.isHalted() &&
                            !vertex.getMsgList().isEmpty()) {
                        Vertex<I, V, E, M> activatedVertex =
                            (Vertex<I, V, E, M>) vertex;
                        activatedVertex.halt = false;
                    }
                    if (!vertex.isHalted()) {
                        Iterator<M> vertexMsgIt =
                            vertex.getMsgList().iterator();
                        context.progress();
                        vertex.compute(vertexMsgIt);
                    }
                    if (vertex.isHalted()) {
                        ++workerFinishedVertices;
                    }
                    ++workerVertices;
                    workerEdges += vertex.getOutEdgeMap().size();
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
            workerSentMessages = commService.flush(context);
        } while (!serviceWorker.finishSuperstep(workerFinishedVertices,
                                                workerVertices,
                                                workerEdges,
                                                workerSentMessages));
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
            LOG.info("cleanup: Starting for " + getMapFunctions());
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
        if (zkManager != null) {
            zkManager.offlineZooKeeperServers(
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
