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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.log4j.PatternLayout;

/**
 * This mapper that will execute the BSP graph tasks.  Since this mapper will
 * not be passing data by key-value pairs through the MR framework, the
 * types are irrelevant.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class GraphMapper<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable> extends
    Mapper<Object, Object, Object, Object> {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GraphMapper.class);
  /** Coordination service worker */
  private CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Coordination service master */
  private CentralizedServiceMaster<I, V, E, M> serviceMaster;
  /** Coordination service master thread */
  private Thread masterThread = null;
  /** The map should be run exactly once, or else there is a problem. */
  private boolean mapAlreadyRun = false;
  /** Manages the ZooKeeper servers if necessary (dynamic startup) */
  private ZooKeeperManager zkManager;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Already complete? */
  private boolean done = false;
  /** What kind of functions is this mapper doing? */
  private MapFunctions mapFunctions = MapFunctions.UNKNOWN;
  /** Total number of vertices in the graph (at this time) */
  private long numVertices = -1;
  /** Total number of edges in the graph (at this time) */
  private long numEdges = -1;

  /** What kinds of functions to run on this mapper */
  public enum MapFunctions {
    /** Undecided yet */
    UNKNOWN,
    /** Only be the master */
    MASTER_ONLY,
    /** Only be the master and ZooKeeper */
    MASTER_ZOOKEEPER_ONLY,
    /** Only be the worker */
    WORKER_ONLY,
    /** Do master, worker, and ZooKeeper */
    ALL,
    /** Do master and worker */
    ALL_EXCEPT_ZOOKEEPER
  }

  /**
   * Get the map function enum.
   *
   * @return Map functions of this mapper.
   */
  public MapFunctions getMapFunctions() {
    return mapFunctions;
  }

  /**
   * Get worker aggregator usage, a subset of the functionality
   *
   * @return Worker aggregator usage interface
   */
  public final WorkerAggregatorUsage getWorkerAggregatorUsage() {
    return serviceWorker.getAggregatorUsage();
  }

  /**
   * Get master aggregator usage, a subset of the functionality
   *
   * @return Master aggregator usage interface
   */
  public final MasterAggregatorUsage getMasterAggregatorUsage() {
    return serviceMaster.getAggregatorUsage();
  }

  public final WorkerContext getWorkerContext() {
    return serviceWorker.getWorkerContext();
  }

  /**
   * Default handler for uncaught exceptions.
   */
  class OverrideExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.fatal(
          "uncaughtException: OverrideExceptionHandler on thread " +
              t.getName() + ", msg = " +  e.getMessage() + ", exiting...", e);
      System.exit(1);
    }
  }

  /**
   * Set the concrete, user-defined choices about generic methods
   * (validated earlier in GiraphRunner) into the Configuration.
   * @param conf the Configuration object for this job run.
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
    conf.setClass(GiraphConfiguration.VERTEX_ID_CLASS,
        (Class<?>) vertexIndexType,
        WritableComparable.class);
    conf.setClass(GiraphConfiguration.VERTEX_VALUE_CLASS,
        (Class<?>) vertexValueType,
        Writable.class);
    conf.setClass(GiraphConfiguration.EDGE_VALUE_CLASS,
        (Class<?>) edgeValueType,
        Writable.class);
    conf.setClass(GiraphConfiguration.MESSAGE_VALUE_CLASS,
        (Class<?>) messageValueType,
        Writable.class);
  }

    /**
    * Copied from JobConf to get the location of this jar.  Workaround for
    * things like Oozie map-reduce jobs.
    *
    * @param myClass Class to search the class loader path for to locate
    *        the relevant jar file
    * @return Location of the jar file containing myClass
    */
  private static String findContainingJar(Class<?> myClass) {
    ClassLoader loader = myClass.getClassLoader();
    String classFile =
        myClass.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<?> itr = loader.getResources(classFile);
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
   * Figure out what functions this mapper should do.  Basic logic is as
   * follows:
   * 1) If not split master, everyone does the everything and/or running
   *    ZooKeeper.
   * 2) If split master/worker, masters also run ZooKeeper (if it's not
   *    given to us).
   *
   * @param conf Configuration to use
   * @param zkManager ZooKeeper manager to help determine whether to run
   *        ZooKeeper
   * @return Functions that this mapper should do.
   */
  private static MapFunctions determineMapFunctions(
      ImmutableClassesGiraphConfiguration conf,
      ZooKeeperManager zkManager) {
    boolean splitMasterWorker = conf.getSplitMasterWorker();
    int taskPartition = conf.getTaskPartition();
    boolean zkAlreadyProvided = conf.getZookeeperList() != null;
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
        int masterCount = conf.getZooKeeperServerCount();
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
    determineClassTypes(context.getConfiguration());
    conf = new ImmutableClassesGiraphConfiguration<I, V, E, M>(
        context.getConfiguration());
    // Hadoop security needs this property to be set
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      conf.set("mapreduce.job.credentials.binary",
          System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    // Set the log level
    String logLevel = conf.getLocalLevel();
    Logger.getRootLogger().setLevel(Level.toLevel(logLevel));
    if (LOG.isInfoEnabled()) {
      LOG.info("setup: Set log level to " + logLevel);
    }
    // Sets pattern layout for all appenders
    if (conf.useLogThreadLayout()) {
      PatternLayout layout =
          new PatternLayout("%-7p %d [%t] %c %x - %m%n");
      Enumeration<Appender> appenderEnum =
          Logger.getRootLogger().getAllAppenders();
      while (appenderEnum.hasMoreElements()) {
        appenderEnum.nextElement().setLayout(layout);
      }
    }

    // Do some initial setup (possibly starting up a Zookeeper service)
    context.setStatus("setup: Initializing Zookeeper services.");
    if (!conf.getLocalTestMode()) {
      Path[] fileClassPaths = DistributedCache.getLocalCacheArchives(conf);
      String zkClasspath = null;
      if (fileClassPaths == null) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Distributed cache is empty. Assuming fatjar.");
        }
        String jarFile = context.getJar();
        if (jarFile == null) {
          jarFile = findContainingJar(getClass());
        }
        zkClasspath = jarFile.replaceFirst("file:", "");
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append(fileClassPaths[0]);

        for (int i = 1; i < fileClassPaths.length; i++) {
          sb.append(":");
          sb.append(fileClassPaths[i]);
        }
        zkClasspath = sb.toString();
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("setup: classpath @ " + zkClasspath + " for job " +
            context.getJobName());
      }
      conf.setZooKeeperJar(zkClasspath);
    }
    String serverPortList = conf.getZookeeperList();
    if (serverPortList == null) {
      zkManager = new ZooKeeperManager(context, conf);
      context.setStatus("setup: Setting up Zookeeper manager.");
      zkManager.setup();
      if (zkManager.computationDone()) {
        done = true;
        return;
      }
      zkManager.onlineZooKeeperServers();
      serverPortList = zkManager.getZooKeeperServerPortString();
    }
    if (zkManager != null && zkManager.runsZooKeeper()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Chosen to run ZooKeeper...");
      }
    }
    context.setStatus("setup: Connected to Zookeeper service " +
        serverPortList);
    this.mapFunctions = determineMapFunctions(conf, zkManager);

    // Sometimes it takes a while to get multiple ZooKeeper servers up
    if (conf.getZooKeeperServerCount() > 1) {
      Thread.sleep(GiraphConfiguration.DEFAULT_ZOOKEEPER_INIT_LIMIT *
          GiraphConfiguration.DEFAULT_ZOOKEEPER_TICK_TIME);
    }
    int sessionMsecTimeout = conf.getZooKeeperSessionTimeout();
    try {
      if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
          (mapFunctions == MapFunctions.MASTER_ONLY) ||
          (mapFunctions == MapFunctions.ALL) ||
          (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Starting up BspServiceMaster " +
              "(master thread)...");
        }
        serviceMaster = new BspServiceMaster<I, V, E, M>(
            serverPortList, sessionMsecTimeout, context, this);
        masterThread = new MasterThread<I, V, E, M>(
            (BspServiceMaster<I, V, E, M>) serviceMaster, context);
        masterThread.start();
      }
      if ((mapFunctions == MapFunctions.WORKER_ONLY) ||
          (mapFunctions == MapFunctions.ALL) ||
          (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Starting up BspServiceWorker...");
        }
        serviceWorker = new BspServiceWorker<I, V, E, M>(
            serverPortList,
            sessionMsecTimeout,
            context,
            this);
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Registering health of this worker...");
        }
      }
    } catch (IOException e) {
      LOG.error("setup: Caught exception just before end of setup", e);
      if (zkManager != null) {
        zkManager.offlineZooKeeperServers(
            ZooKeeperManager.State.FAILED);
      }
      throw new RuntimeException(
          "setup: Offlining servers due to exception...", e);
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
    if (done) {
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
      throw new RuntimeException("map: In BSP, map should have only been" +
          " run exactly once, (already run)");
    }
    mapAlreadyRun = true;

    FinishedSuperstepStats inputSuperstepStats =
        serviceWorker.setup();
    numVertices = inputSuperstepStats.getVertexCount();
    numEdges = inputSuperstepStats.getEdgeCount();
    if (inputSuperstepStats.getVertexCount() == 0) {
      LOG.warn("map: No vertices in the graph, exiting.");
      return;
    }

    serviceWorker.getWorkerContext().setGraphState(
        new GraphState<I, V, E, M>(serviceWorker.getSuperstep(),
            numVertices, numEdges, context, this, null));
    try {
      serviceWorker.getWorkerContext().preApplication();
    } catch (InstantiationException e) {
      LOG.fatal("map: preApplication failed in instantiation", e);
      throw new RuntimeException(
          "map: preApplication failed in instantiation", e);
    } catch (IllegalAccessException e) {
      LOG.fatal("map: preApplication failed in access", e);
      throw new RuntimeException(
          "map: preApplication failed in access", e);
    }
    context.progress();

    List<PartitionStats> partitionStatsList =
        new ArrayList<PartitionStats>();

    int numThreads = conf.getNumComputeThreads();
    FinishedSuperstepStats finishedSuperstepStats = null;
    do {
      final long superstep = serviceWorker.getSuperstep();

      GraphState<I, V, E, M> graphState =
          new GraphState<I, V, E, M>(superstep, numVertices, numEdges,
              context, this, null);

      Collection<? extends PartitionOwner> masterAssignedPartitionOwners =
          serviceWorker.startSuperstep(graphState);

      if (LOG.isDebugEnabled()) {
        LOG.debug("map: " + MemoryUtils.getRuntimeMemoryStats());
      }
      context.progress();

      serviceWorker.exchangeVertexPartitions(
          masterAssignedPartitionOwners);
      context.progress();

      // Might need to restart from another superstep
      // (manually or automatic), or store a checkpoint
      if (serviceWorker.getRestartedSuperstep() == superstep) {
        if (LOG.isInfoEnabled()) {
          LOG.info("map: Loading from checkpoint " + superstep);
        }
        VertexEdgeCount vertexEdgeCount = serviceWorker.loadCheckpoint(
            serviceWorker.getRestartedSuperstep());
        numVertices = vertexEdgeCount.getVertexCount();
        numEdges = vertexEdgeCount.getEdgeCount();
        graphState = new GraphState<I, V, E, M>(superstep, numVertices,
            numEdges, context, this, null);
      } else if (serviceWorker.checkpointFrequencyMet(superstep)) {
        serviceWorker.storeCheckpoint();
      }

      serviceWorker.getWorkerContext().setGraphState(graphState);
      serviceWorker.getWorkerContext().preSuperstep();
      context.progress();

      MessageStoreByPartition<I, M> messageStore =
          serviceWorker.getServerData().getCurrentMessageStore();
      partitionStatsList.clear();
      int numPartitions = serviceWorker.getPartitionStore().getNumPartitions();
      if (LOG.isInfoEnabled()) {
        LOG.info("map: " + numPartitions +
            " partitions to process in superstep " + superstep + " with " +
            numThreads + " thread(s)");
      }

      if (numPartitions > 0) {
        List<Future<Collection<PartitionStats>>> partitionFutures =
            Lists.newArrayListWithCapacity(numPartitions);
        BlockingQueue<Integer> computePartitionIdQueue =
            new ArrayBlockingQueue<Integer>(numPartitions);
        for (Integer partitionId :
            serviceWorker.getPartitionStore().getPartitionIds()) {
          computePartitionIdQueue.add(partitionId);
        }

        ExecutorService partitionExecutor =
            Executors.newFixedThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("compute-%d").build());
        for (int i = 0; i < numThreads; ++i) {
          ComputeCallable<I, V, E, M> computeCallable =
              new ComputeCallable<I, V, E, M>(
                  context,
                  graphState,
                  messageStore,
                  computePartitionIdQueue,
                  conf,
                  serviceWorker);
          partitionFutures.add(partitionExecutor.submit(computeCallable));
        }

        // Wait until all the threads are done to wait on all requests
        for (Future<Collection<PartitionStats>> partitionFuture :
            partitionFutures) {
          Collection<PartitionStats> stats =
              ProgressableUtils.getFutureResult(partitionFuture, context);
          partitionStatsList.addAll(stats);
        }
        partitionExecutor.shutdown();
      }

      finishedSuperstepStats =
          serviceWorker.finishSuperstep(graphState, partitionStatsList);
      numVertices = finishedSuperstepStats.getVertexCount();
      numEdges = finishedSuperstepStats.getEdgeCount();
    } while (!finishedSuperstepStats.getAllVerticesHalted());
    if (LOG.isInfoEnabled()) {
      LOG.info("map: BSP application done (global vertices marked done)");
    }

    serviceWorker.getWorkerContext().setGraphState(
        new GraphState<I, V, E, M>(serviceWorker.getSuperstep(),
            numVertices, numEdges, context, this, null));
    serviceWorker.getWorkerContext().postApplication();
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
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    // Notify the master quicker if there is worker failure rather than
    // waiting for ZooKeeper to timeout and delete the ephemeral znodes
    try {
      setup(context);
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(),
            context.getCurrentValue(),
            context);
      }
      cleanup(context);
      // Checkstyle exception due to needing to dump ZooKeeper failure
      // on exception
      // CHECKSTYLE: stop IllegalCatch
    } catch (RuntimeException e) {
      // CHECKSTYLE: resume IllegalCatch
      if (mapFunctions == MapFunctions.UNKNOWN ||
          mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) {
        // ZooKeeper may have had an issue
        if (zkManager != null) {
          zkManager.logZooKeeperOutput(Level.WARN);
        }
      }
      if (mapFunctions == MapFunctions.WORKER_ONLY) {
        serviceWorker.failureCleanup();
      }
      throw new IllegalStateException(
          "run: Caught an unrecoverable exception " + e.getMessage(), e);
    }
  }
}
