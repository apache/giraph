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

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.yammer.metrics.core.Counter;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.job.JobProgressTracker;
import org.apache.giraph.master.BspServiceMaster;
import org.apache.giraph.master.MasterThread;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphMetricsRegistry;
import org.apache.giraph.metrics.GiraphTimer;
import org.apache.giraph.metrics.GiraphTimerContext;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.GcObserver;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.InputSplitsCallable;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

/**
 * The Giraph-specific business logic for a single BSP
 * compute node in whatever underlying type of cluster
 * our Giraph job will run on. Owning object will provide
 * the glue into the underlying cluster framework
 * and will call this object to perform Giraph work.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class GraphTaskManager<I extends WritableComparable, V extends Writable,
  E extends Writable> implements
  ResetSuperstepMetricsObserver {
/*if_not[PURE_YARN]
  static { // Eliminate this? Even MRv1 tasks should not need it here.
    Configuration.addDefaultResource("giraph-site.xml");
  }
end[PURE_YARN]*/
  /**
   * Class which checks if an exception on some thread should cause worker
   * to fail
   */
  public static final ClassConfOption<CheckerIfWorkerShouldFailAfterException>
  CHECKER_IF_WORKER_SHOULD_FAIL_AFTER_EXCEPTION_CLASS = ClassConfOption.create(
      "giraph.checkerIfWorkerShouldFailAfterExceptionClass",
      FailWithEveryExceptionOccurred.class,
      CheckerIfWorkerShouldFailAfterException.class,
      "Class which checks if an exception on some thread should cause worker " +
          "to fail, by default all exceptions cause failure");
  /** Name of metric for superstep time in msec */
  public static final String TIMER_SUPERSTEP_TIME = "superstep-time-ms";
  /** Name of metric for compute on all vertices in msec */
  public static final String TIMER_COMPUTE_ALL = "compute-all-ms";
  /** Name of metric for time from begin compute to first message sent */
  public static final String TIMER_TIME_TO_FIRST_MSG =
      "time-to-first-message-ms";
  /** Name of metric for time from first message till last message flushed */
  public static final String TIMER_COMMUNICATION_TIME = "communication-time-ms";
  /** Name of metric for time spent doing GC per superstep in msec */
  public static final String TIMER_SUPERSTEP_GC_TIME = "superstep-gc-time-ms";

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GraphTaskManager.class);
  /** Coordination service worker */
  private CentralizedServiceWorker<I, V, E> serviceWorker;
  /** Coordination service master */
  private CentralizedServiceMaster<I, V, E> serviceMaster;
  /** Coordination service master thread */
  private Thread masterThread = null;
  /** The worker should be run exactly once, or else there is a problem. */
  private boolean alreadyRun = false;
  /** Manages the ZooKeeper servers if necessary (dynamic startup) */
  private ZooKeeperManager zkManager;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Already complete? */
  private boolean done = false;
  /** What kind of functions is this mapper doing? */
  private GraphFunctions graphFunctions = GraphFunctions.UNKNOWN;
  /** Superstep stats */
  private FinishedSuperstepStats finishedSuperstepStats =
      new FinishedSuperstepStats(0, false, 0, 0, false, CheckpointStatus.NONE);
  /** Job progress tracker */
  private JobProgressTrackerClient jobProgressTracker;

  // Per-Job Metrics
  /** Timer for WorkerContext#preApplication() */
  private GiraphTimer wcPreAppTimer;
  /** Timer for WorkerContext#postApplication() */
  private GiraphTimer wcPostAppTimer;

  // Per-Superstep Metrics
  /** Time for how long superstep took */
  private GiraphTimer superstepTimer;
  /** Time for all compute() calls in a superstep */
  private GiraphTimer computeAll;
  /** Time from starting compute to sending first message */
  private GiraphTimer timeToFirstMessage;
  /** Context for timing time to first message above */
  private GiraphTimerContext timeToFirstMessageTimerContext;
  /** Time from first sent message till last message flushed. */
  private GiraphTimer communicationTimer;
  /** Context for timing communication time above */
  private GiraphTimerContext communicationTimerContext;
  /** Timer for WorkerContext#preSuperstep() */
  private GiraphTimer wcPreSuperstepTimer;
  /** Timer to keep aggregated time spent in GC in a superstep */
  private Counter gcTimeMetric;
  /** The Hadoop Mapper#Context for this job */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** is this GraphTaskManager the master? */
  private boolean isMaster;
  /** Mapper observers */
  private MapperObserver[] mapperObservers;

  /**
   * Default constructor for GiraphTaskManager.
   * @param context a handle to the underlying cluster framework.
   *                For Hadoop clusters, this is a Mapper#Context.
   */
  public GraphTaskManager(Mapper<?, ?, ?, ?>.Context context) {
    this.context = context;
    this.isMaster = false;
  }

  /**
   * Run the user's input checking code.
   */
  private void checkInput() {
    if (conf.hasEdgeInputFormat()) {
      conf.createWrappedEdgeInputFormat().checkInputSpecs(conf);
    }
    if (conf.hasVertexInputFormat()) {
      conf.createWrappedVertexInputFormat().checkInputSpecs(conf);
    }
  }

  /**
   * In order for job client to know which ZooKeeper the job is using,
   * we create a counter with server:port as its name inside of
   * ZOOKEEPER_SERVER_PORT_COUNTER_GROUP.
   *
   * @param serverPortList Server:port list for ZooKeeper used
   */
  private void createZooKeeperCounter(String serverPortList) {
    // Getting the counter will actually create it.
    context.getCounter(GiraphConstants.ZOOKEEPER_SERVER_PORT_COUNTER_GROUP,
        serverPortList);
  }

  /**
   * Called by owner of this GraphTaskManager on each compute node
   *
   * @param zkPathList the path to the ZK jars we need to run the job
   */
  public void setup(Path[] zkPathList)
    throws IOException, InterruptedException {
    context.setStatus("setup: Beginning worker setup.");
    Configuration hadoopConf = context.getConfiguration();
    conf = new ImmutableClassesGiraphConfiguration<I, V, E>(hadoopConf);
    initializeJobProgressTracker();
    // Setting the default handler for uncaught exceptions.
    Thread.setDefaultUncaughtExceptionHandler(createUncaughtExceptionHandler());
    setupMapperObservers();
    // Write user's graph types (I,V,E,M) back to configuration parameters so
    // that they are set for quicker access later. These types are often
    // inferred from the Computation class used.
    conf.getGiraphTypes().writeIfUnset(conf);
    // configure global logging level for Giraph job
    initializeAndConfigureLogging();
    // init the metrics objects
    setupAndInitializeGiraphMetrics();
    // Check input
    checkInput();
    // Load any scripts that were deployed
    ScriptLoader.loadScripts(conf);
    // One time setup for computation factory
    conf.createComputationFactory().initialize(conf);
    // Do some task setup (possibly starting up a Zookeeper service)
    context.setStatus("setup: Initializing Zookeeper services.");
    String serverPortList = conf.getZookeeperList();
    if (serverPortList.isEmpty()) {
      if (startZooKeeperManager()) {
        return; // ZK connect/startup failed
      }
    } else {
      createZooKeeperCounter(serverPortList);
    }
    if (zkManager != null && zkManager.runsZooKeeper()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Chosen to run ZooKeeper...");
      }
    }
    context
        .setStatus("setup: Connected to Zookeeper service " + serverPortList);
    this.graphFunctions = determineGraphFunctions(conf, zkManager);
    if (zkManager != null && this.graphFunctions.isMaster()) {
      zkManager.cleanupOnExit();
    }
    try {
      instantiateBspService();
    } catch (IOException e) {
      LOG.error("setup: Caught exception just before end of setup", e);
      if (zkManager != null) {
        zkManager.offlineZooKeeperServers(ZooKeeperManager.State.FAILED);
      }
      throw new RuntimeException(
        "setup: Offlining servers due to exception...", e);
    }
    context.setStatus(getGraphFunctions().toString() + " starting...");
  }

  /**
   * Create and connect a client to JobProgressTrackerService,
   * or no-op implementation if progress shouldn't be tracked or something
   * goes wrong
   */
  private void initializeJobProgressTracker() {
    if (!conf.trackJobProgressOnClient()) {
      jobProgressTracker = new JobProgressTrackerClientNoOp();
    } else {
      try {
        jobProgressTracker = new RetryableJobProgressTrackerClient(conf);
      } catch (InterruptedException | ExecutionException e) {
        LOG.warn("createJobProgressClient: Exception occurred while trying to" +
            " connect to JobProgressTracker - not reporting progress", e);
        jobProgressTracker = new JobProgressTrackerClientNoOp();
      }
    }
    jobProgressTracker.mapperStarted();
  }

  /**
  * Perform the work assigned to this compute node for this job run.
  * 1) Run checkpoint per frequency policy.
  * 2) For every vertex on this mapper, run the compute() function
  * 3) Wait until all messaging is done.
  * 4) Check if all vertices are done.  If not goto 2).
  * 5) Dump output.
  */
  public void execute() throws IOException, InterruptedException {
    if (checkTaskState()) {
      return;
    }
    preLoadOnWorkerObservers();
    GiraphTimerContext superstepTimerContext = superstepTimer.time();
    finishedSuperstepStats = serviceWorker.setup();
    superstepTimerContext.stop();
    if (collectInputSuperstepStats(finishedSuperstepStats)) {
      return;
    }
    prepareGraphStateAndWorkerContext();
    List<PartitionStats> partitionStatsList = new ArrayList<PartitionStats>();
    int numComputeThreads = conf.getNumComputeThreads();

    // main superstep processing loop
    while (!finishedSuperstepStats.allVerticesHalted()) {
      final long superstep = serviceWorker.getSuperstep();
      superstepTimerContext = getTimerForThisSuperstep(superstep);
      GraphState graphState = new GraphState(superstep,
          finishedSuperstepStats.getVertexCount(),
          finishedSuperstepStats.getEdgeCount(),
          context);
      Collection<? extends PartitionOwner> masterAssignedPartitionOwners =
        serviceWorker.startSuperstep();
      if (LOG.isDebugEnabled()) {
        LOG.debug("execute: " + MemoryUtils.getRuntimeMemoryStats());
      }
      context.progress();
      serviceWorker.exchangeVertexPartitions(masterAssignedPartitionOwners);
      context.progress();
      boolean hasBeenRestarted = checkSuperstepRestarted(superstep);

      GlobalStats globalStats = serviceWorker.getGlobalStats();

      if (hasBeenRestarted) {
        graphState = new GraphState(superstep,
            finishedSuperstepStats.getVertexCount(),
            finishedSuperstepStats.getEdgeCount(),
            context);
      } else if (storeCheckpoint(globalStats.getCheckpointStatus())) {
        break;
      }
      serviceWorker.getServerData().prepareResolveMutations();
      context.progress();
      prepareForSuperstep(graphState);
      context.progress();
      MessageStore<I, Writable> messageStore =
          serviceWorker.getServerData().getCurrentMessageStore();
      int numPartitions = serviceWorker.getPartitionStore().getNumPartitions();
      int numThreads = Math.min(numComputeThreads, numPartitions);
      if (LOG.isInfoEnabled()) {
        LOG.info("execute: " + numPartitions + " partitions to process with " +
          numThreads + " compute thread(s), originally " +
          numComputeThreads + " thread(s) on superstep " + superstep);
      }
      partitionStatsList.clear();
      // execute the current superstep
      if (numPartitions > 0) {
        processGraphPartitions(context, partitionStatsList, graphState,
          messageStore, numThreads);
      }
      finishedSuperstepStats = completeSuperstepAndCollectStats(
        partitionStatsList, superstepTimerContext);

      // END of superstep compute loop
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("execute: BSP application done (global vertices marked done)");
    }
    updateSuperstepGraphState();
    postApplication();
  }

  /**
   * Handle post-application callbacks.
   */
  private void postApplication() throws IOException, InterruptedException {
    GiraphTimerContext postAppTimerContext = wcPostAppTimer.time();
    serviceWorker.getWorkerContext().postApplication();
    serviceWorker.getSuperstepOutput().postApplication();
    postAppTimerContext.stop();
    context.progress();

    for (WorkerObserver obs : serviceWorker.getWorkerObservers()) {
      obs.postApplication();
      context.progress();
    }
  }

  /**
   * Sets the "isMaster" flag for final output commit to happen on master.
   * @param im the boolean input to set isMaster. Applies to "pure YARN only"
   */
  public void setIsMaster(final boolean im) {
    this.isMaster = im;
  }

  /**
   * Get "isMaster" status flag -- we need to know if we're the master in the
   * "finally" block of our GiraphYarnTask#execute() to commit final job output.
   * @return true if this task IS the master.
   */
  public boolean isMaster() {
    return isMaster;
  }

  /**
   * Produce a reference to the "start" superstep timer for the current
   * superstep.
   * @param superstep the current superstep count
   * @return a GiraphTimerContext representing the "start" of the supestep
   */
  private GiraphTimerContext getTimerForThisSuperstep(long superstep) {
    GiraphMetrics.get().resetSuperstepMetrics(superstep);
    return superstepTimer.time();
  }

  /**
   * Utility to encapsulate Giraph metrics setup calls
   */
  private void setupAndInitializeGiraphMetrics() {
    GiraphMetrics.init(conf);
    GiraphMetrics.get().addSuperstepResetObserver(this);
    initJobMetrics();
    MemoryUtils.initMetrics();
    InputSplitsCallable.initMetrics();
  }

  /**
   * Instantiate and configure ZooKeeperManager for this job. This will
   * result in a Giraph-owned Zookeeper instance, a connection to an
   * existing quorum as specified in the job configuration, or task failure
   * @return true if this task should terminate
   */
  private boolean startZooKeeperManager()
    throws IOException, InterruptedException {
    zkManager = new ZooKeeperManager(context, conf);
    context.setStatus("setup: Setting up Zookeeper manager.");
    zkManager.setup();
    if (zkManager.computationDone()) {
      done = true;
      return true;
    }
    zkManager.onlineZooKeeperServer();
    String serverPortList = zkManager.getZooKeeperServerPortString();
    conf.setZookeeperList(serverPortList);
    createZooKeeperCounter(serverPortList);
    return false;
  }

  /**
   * Utility to place a new, updated GraphState object into the serviceWorker.
   */
  private void updateSuperstepGraphState() {
    serviceWorker.getWorkerContext().setGraphState(
        new GraphState(serviceWorker.getSuperstep(),
            finishedSuperstepStats.getVertexCount(),
            finishedSuperstepStats.getEdgeCount(), context));
  }

  /**
   * Utility function for boilerplate updates and cleanup done at the
   * end of each superstep processing loop in the <code>execute</code> method.
   * @param partitionStatsList list of stas for each superstep to append to
   * @param superstepTimerContext for job metrics
   * @return the collected stats at the close of the current superstep.
   */
  private FinishedSuperstepStats completeSuperstepAndCollectStats(
    List<PartitionStats> partitionStatsList,
    GiraphTimerContext superstepTimerContext) {

    // the superstep timer is stopped inside the finishSuperstep function
    // (otherwise metrics are not available at the end of the computation
    //  using giraph.metrics.enable=true).
    finishedSuperstepStats =
      serviceWorker.finishSuperstep(partitionStatsList, superstepTimerContext);
    if (conf.metricsEnabled()) {
      GiraphMetrics.get().perSuperstep().printSummary(System.err);
    }
    return finishedSuperstepStats;
  }

  /**
   * Utility function to prepare various objects managing BSP superstep
   * operations for the next superstep.
   * @param graphState graph state metadata object
   */
  private void prepareForSuperstep(GraphState graphState) {
    serviceWorker.prepareSuperstep();

    serviceWorker.getWorkerContext().setGraphState(graphState);
    serviceWorker.getWorkerContext().setupSuperstep(serviceWorker);
    GiraphTimerContext preSuperstepTimer = wcPreSuperstepTimer.time();
    serviceWorker.getWorkerContext().preSuperstep();
    preSuperstepTimer.stop();
    context.progress();

    for (WorkerObserver obs : serviceWorker.getWorkerObservers()) {
      obs.preSuperstep(graphState.getSuperstep());
      context.progress();
    }
  }

  /**
   * Prepare graph state and worker context for superstep cycles.
   */
  private void prepareGraphStateAndWorkerContext() {
    updateSuperstepGraphState();
    workerContextPreApp();
  }

  /**
    * Get the worker function enum.
    *
    * @return an enum detailing the roles assigned to this
    *         compute node for this Giraph job.
    */
  public GraphFunctions getGraphFunctions() {
    return graphFunctions;
  }

  public final WorkerContext getWorkerContext() {
    return serviceWorker.getWorkerContext();
  }

  public JobProgressTracker getJobProgressTracker() {
    return jobProgressTracker;
  }

  /**
   * Copied from JobConf to get the location of this jar.  Workaround for
   * things like Oozie map-reduce jobs. NOTE: Pure YARN profile cannot
   * make use of this, as the jars are unpacked at each container site.
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
   * Figure out what roles this BSP compute node should take on in the job.
   * Basic logic is as follows:
   * 1) If not split master, everyone does the everything and/or running
   *    ZooKeeper.
   * 2) If split master/worker, masters also run ZooKeeper
   *
   * 3) If split master/worker == true and <code>giraph.zkList</code> is
   *    externally provided, the master will not instantiate a ZK instance, but
   *    will assume a quorum is already active on the cluster for Giraph to use.
   *
   * @param conf Configuration to use
   * @param zkManager ZooKeeper manager to help determine whether to run
   *        ZooKeeper.
   * @return Functions that this mapper should do.
   */
  private static GraphFunctions determineGraphFunctions(
      ImmutableClassesGiraphConfiguration conf,
      ZooKeeperManager zkManager) {
    boolean splitMasterWorker = conf.getSplitMasterWorker();
    int taskPartition = conf.getTaskPartition();
    boolean zkAlreadyProvided = conf.isZookeeperExternal();
    GraphFunctions functions = GraphFunctions.UNKNOWN;
    // What functions should this mapper do?
    if (!splitMasterWorker) {
      if ((zkManager != null) && zkManager.runsZooKeeper()) {
        functions = GraphFunctions.ALL;
      } else {
        functions = GraphFunctions.ALL_EXCEPT_ZOOKEEPER;
      }
    } else {
      if (zkAlreadyProvided) {
        if (taskPartition == 0) {
          functions = GraphFunctions.MASTER_ONLY;
        } else {
          functions = GraphFunctions.WORKER_ONLY;
        }
      } else {
        if ((zkManager != null) && zkManager.runsZooKeeper()) {
          functions = GraphFunctions.MASTER_ZOOKEEPER_ONLY;
        } else {
          functions = GraphFunctions.WORKER_ONLY;
        }
      }
    }
    return functions;
  }

  /**
   * Instantiate the appropriate BspService object (Master or Worker)
   * for this compute node.
   */
  private void instantiateBspService()
    throws IOException, InterruptedException {
    if (graphFunctions.isMaster()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Starting up BspServiceMaster " +
          "(master thread)...");
      }
      serviceMaster = new BspServiceMaster<I, V, E>(context, this);
      masterThread = new MasterThread<I, V, E>(serviceMaster, context);
      masterThread.setUncaughtExceptionHandler(
          createUncaughtExceptionHandler());
      masterThread.start();
    }
    if (graphFunctions.isWorker()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Starting up BspServiceWorker...");
      }
      serviceWorker = new BspServiceWorker<I, V, E>(context, this);
      installGCMonitoring();
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Registering health of this worker...");
      }
    }
  }

  /**
   * Install GC monitoring. This method intercepts all GC, log the gc, and
   * notifies an out-of-core engine (if any is used) about the GC.
   */
  private void installGCMonitoring() {
    final GcObserver[] gcObservers = conf.createGcObservers(context);
    List<GarbageCollectorMXBean> mxBeans = ManagementFactory
        .getGarbageCollectorMXBeans();
    final OutOfCoreEngine oocEngine =
        serviceWorker.getServerData().getOocEngine();
    for (GarbageCollectorMXBean gcBean : mxBeans) {
      NotificationEmitter emitter = (NotificationEmitter) gcBean;
      NotificationListener listener = new NotificationListener() {
        @Override
        public void handleNotification(Notification notification,
                                       Object handle) {
          if (notification.getType().equals(GarbageCollectionNotificationInfo
              .GARBAGE_COLLECTION_NOTIFICATION)) {
            GarbageCollectionNotificationInfo info =
                GarbageCollectionNotificationInfo.from(
                    (CompositeData) notification.getUserData());

            if (LOG.isInfoEnabled()) {
              LOG.info("installGCMonitoring: name = " + info.getGcName() +
                  ", action = " + info.getGcAction() + ", cause = " +
                  info.getGcCause() + ", duration = " +
                  info.getGcInfo().getDuration() + "ms");
            }
            gcTimeMetric.inc(info.getGcInfo().getDuration());
            GiraphMetrics.get().getGcTracker().gcOccurred(info.getGcInfo());
            for (GcObserver gcObserver : gcObservers) {
              gcObserver.gcOccurred(info);
            }
            if (oocEngine != null) {
              oocEngine.gcCompleted(info);
            }
          }
        }
      };
      //Add the listener
      emitter.addNotificationListener(listener, null, null);
    }
  }

  /**
   * Initialize the root logger and appender to the settings in conf.
   */
  private void initializeAndConfigureLogging() {
    // Set the log level
    String logLevel = conf.getLocalLevel();
    if (!Logger.getRootLogger().getLevel().equals(Level.toLevel(logLevel))) {
      Logger.getRootLogger().setLevel(Level.toLevel(logLevel));
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Set log level to " + logLevel);
      }
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Log level remains at " + logLevel);
      }
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
    // Change ZooKeeper logging level to error (info is quite verbose) for
    // testing only
    if (conf.getLocalTestMode()) {
      LogManager.getLogger(org.apache.zookeeper.server.PrepRequestProcessor.
          class.getName()).setLevel(Level.ERROR);
    }
  }

  /**
   * Initialize job-level metrics used by this class.
   */
  private void initJobMetrics() {
    GiraphMetricsRegistry jobMetrics = GiraphMetrics.get().perJobOptional();
    wcPreAppTimer = new GiraphTimer(jobMetrics, "worker-context-pre-app",
        TimeUnit.MILLISECONDS);
    wcPostAppTimer = new GiraphTimer(jobMetrics, "worker-context-post-app",
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    superstepTimer = new GiraphTimer(superstepMetrics,
        TIMER_SUPERSTEP_TIME, TimeUnit.MILLISECONDS);
    computeAll = new GiraphTimer(superstepMetrics,
        TIMER_COMPUTE_ALL, TimeUnit.MILLISECONDS);
    timeToFirstMessage = new GiraphTimer(superstepMetrics,
        TIMER_TIME_TO_FIRST_MSG, TimeUnit.MICROSECONDS);
    communicationTimer = new GiraphTimer(superstepMetrics,
        TIMER_COMMUNICATION_TIME, TimeUnit.MILLISECONDS);
    gcTimeMetric = superstepMetrics.getCounter(TIMER_SUPERSTEP_GC_TIME);
    wcPreSuperstepTimer = new GiraphTimer(superstepMetrics,
        "worker-context-pre-superstep", TimeUnit.MILLISECONDS);
  }

  /**
   * Notification from Vertex that a message has been sent.
   */
  public void notifySentMessages() {
    // We are tracking the time between when the compute started and the first
    // message get sent. We use null to flag that we have already recorded it.
    GiraphTimerContext tmp = timeToFirstMessageTimerContext;
    if (tmp != null) {
      synchronized (timeToFirstMessage) {
        if (timeToFirstMessageTimerContext != null) {
          timeToFirstMessageTimerContext.stop();
          timeToFirstMessageTimerContext = null;
          communicationTimerContext = communicationTimer.time();
        }
      }
    }
  }

  /**
   * Notification of last message flushed. Comes when we finish the superstep
   * and are done waiting for all messages to send.
   */
  public void notifyFinishedCommunication() {
    GiraphTimerContext tmp = communicationTimerContext;
    if (tmp != null) {
      synchronized (communicationTimer) {
        if (communicationTimerContext != null) {
          communicationTimerContext.stop();
          communicationTimerContext = null;
        }
      }
    }
  }

  /**
   * Process graph data partitions active in this superstep.
   * @param context handle to the underlying cluster framework
   * @param partitionStatsList to pick up this superstep's processing stats
   * @param graphState the BSP graph state
   * @param messageStore the messages to be processed in this superstep
   * @param numThreads number of concurrent threads to do processing
   */
  private void processGraphPartitions(final Mapper<?, ?, ?, ?>.Context context,
      List<PartitionStats> partitionStatsList,
      final GraphState graphState,
      final MessageStore<I, Writable> messageStore,
      int numThreads) {
    PartitionStore<I, V, E> partitionStore = serviceWorker.getPartitionStore();
    long verticesToCompute = 0;
    for (Integer partitionId : partitionStore.getPartitionIds()) {
      verticesToCompute += partitionStore.getPartitionVertexCount(partitionId);
    }
    WorkerProgress.get().startSuperstep(
        serviceWorker.getSuperstep(), verticesToCompute,
        serviceWorker.getPartitionStore().getNumPartitions());
    partitionStore.startIteration();

    GiraphTimerContext computeAllTimerContext = computeAll.time();
    timeToFirstMessageTimerContext = timeToFirstMessage.time();

    CallableFactory<Collection<PartitionStats>> callableFactory =
      new CallableFactory<Collection<PartitionStats>>() {
        @Override
        public Callable<Collection<PartitionStats>> newCallable(
            int callableId) {
          return new ComputeCallable<I, V, E, Writable, Writable>(
              context,
              graphState,
              messageStore,
              conf,
              serviceWorker);
        }
      };
    List<Collection<PartitionStats>> results =
        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
            "compute-%d", context);

    for (Collection<PartitionStats> result : results) {
      partitionStatsList.addAll(result);
    }

    computeAllTimerContext.stop();
  }

  /**
   * Handle the event that this superstep is a restart of a failed one.
   * @param superstep current superstep
   * @return the graph state, updated if this is a restart superstep
   */
  private boolean checkSuperstepRestarted(long superstep) throws IOException {
    // Might need to restart from another superstep
    // (manually or automatic), or store a checkpoint
    if (serviceWorker.getRestartedSuperstep() == superstep) {
      if (LOG.isInfoEnabled()) {
        LOG.info("execute: Loading from checkpoint " + superstep);
      }
      VertexEdgeCount vertexEdgeCount = serviceWorker.loadCheckpoint(
        serviceWorker.getRestartedSuperstep());
      finishedSuperstepStats = new FinishedSuperstepStats(0, false,
          vertexEdgeCount.getVertexCount(), vertexEdgeCount.getEdgeCount(),
          false, CheckpointStatus.NONE);
      return true;
    }
    return false;
  }

  /**
   * Check if it's time to checkpoint and actually does checkpointing
   * if it is.
   * @param checkpointStatus master's decision
   * @return true if we need to stop computation after checkpoint
   * @throws IOException
   */
  private boolean storeCheckpoint(CheckpointStatus checkpointStatus)
    throws IOException {
    if (checkpointStatus != CheckpointStatus.NONE) {
      serviceWorker.storeCheckpoint();
    }
    return checkpointStatus == CheckpointStatus.CHECKPOINT_AND_HALT;
  }

  /**
   * Attempt to collect the final statistics on the graph data
   * processed in this superstep by this compute node
   * @param inputSuperstepStats the final graph data stats object for the
   *                            input superstep
   * @return true if the graph data has no vertices (error?) and
   *         this node should terminate
   */
  private boolean collectInputSuperstepStats(
    FinishedSuperstepStats inputSuperstepStats) {
    if (inputSuperstepStats.getVertexCount() == 0 &&
        !inputSuperstepStats.mustLoadCheckpoint()) {
      LOG.warn("map: No vertices in the graph, exiting.");
      return true;
    }
    if (conf.metricsEnabled()) {
      GiraphMetrics.get().perSuperstep().printSummary(System.err);
    }
    return false;
  }

  /**
   * Did the state of this compute node change?
   * @return true if the processing of supersteps should terminate.
   */
  private boolean checkTaskState() {
    if (done) {
      return true;
    }
    GiraphMetrics.get().resetSuperstepMetrics(BspService.INPUT_SUPERSTEP);
    if (graphFunctions.isNotAWorker()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("map: No need to do anything when not a worker");
      }
      return true;
    }
    if (alreadyRun) {
      throw new RuntimeException("map: In BSP, map should have only been" +
        " run exactly once, (already run)");
    }
    alreadyRun = true;
    return false;
  }

  /**
   * Call to the WorkerContext before application begins.
   */
  private void workerContextPreApp() {
    GiraphTimerContext preAppTimerContext = wcPreAppTimer.time();
    try {
      serviceWorker.getWorkerContext().preApplication();
    } catch (InstantiationException e) {
      LOG.fatal("execute: preApplication failed in instantiation", e);
      throw new RuntimeException(
          "execute: preApplication failed in instantiation", e);
    } catch (IllegalAccessException e) {
      LOG.fatal("execute: preApplication failed in access", e);
      throw new RuntimeException(
          "execute: preApplication failed in access", e);
    }
    preAppTimerContext.stop();
    context.progress();

    for (WorkerObserver obs : serviceWorker.getWorkerObservers()) {
      obs.preApplication();
      context.progress();
    }
  }

  /**
   * Setup mapper observers
   */
  public void setupMapperObservers() {
    mapperObservers = conf.createMapperObservers(context);
    for (MapperObserver mapperObserver : mapperObservers) {
      mapperObserver.setup();
    }
  }

  /**
   * Executes preLoad() on worker observers.
   */
  private void preLoadOnWorkerObservers() {
    for (WorkerObserver obs : serviceWorker.getWorkerObservers()) {
      obs.preLoad();
      context.progress();
    }
  }

  /**
   * Executes postSave() on worker observers.
   */
  private void postSaveOnWorkerObservers() {
    for (WorkerObserver obs : serviceWorker.getWorkerObservers()) {
      obs.postSave();
      context.progress();
    }
  }

  /**
   * Called by owner of this GraphTaskManager object on each compute node
   */
  public void cleanup()
    throws IOException, InterruptedException {
    if (LOG.isInfoEnabled()) {
      LOG.info("cleanup: Starting for " + getGraphFunctions());
    }
    jobProgressTracker.cleanup();
    if (done) {
      return;
    }

    if (serviceWorker != null) {
      serviceWorker.cleanup(finishedSuperstepStats);
      postSaveOnWorkerObservers();
    }
    try {
      if (masterThread != null) {
        masterThread.join();
        LOG.info("cleanup: Joined with master thread");
      }
    } catch (InterruptedException e) {
      // cleanup phase -- just log the error
      LOG.error("cleanup: Master thread couldn't join");
    }
    if (zkManager != null) {
      LOG.info("cleanup: Offlining ZooKeeper servers");
      try {
        zkManager.offlineZooKeeperServers(ZooKeeperManager.State.FINISHED);
      // We need this here cause apparently exceptions are eaten by Hadoop
      // when they come from the cleanup lifecycle and it's useful to know
      // if something is wrong.
      //
      // And since it's cleanup nothing too bad should happen if we don't
      // propagate and just allow the job to finish normally.
      // CHECKSTYLE: stop IllegalCatch
      } catch (Throwable e) {
      // CHECKSTYLE: resume IllegalCatch
        LOG.error("cleanup: Error offlining zookeeper", e);
      }
    }

    // Stop tracking metrics
    GiraphMetrics.get().shutdown();
  }

  /**
   * Cleanup a ZooKeeper instance managed by this
   * GiraphWorker upon job run failure.
   */
  public void zooKeeperCleanup() {
    if (graphFunctions.isZooKeeper()) {
      // ZooKeeper may have had an issue
      if (zkManager != null) {
        zkManager.cleanup();
      }
    }
  }

  /**
   * Cleanup all of Giraph's framework-agnostic resources
   * regardless of which type of cluster Giraph is running on.
   */
  public void workerFailureCleanup() {
    try {
      if (graphFunctions.isWorker()) {
        serviceWorker.failureCleanup();
      }
      // Stop tracking metrics
      GiraphMetrics.get().shutdown();
    // Checkstyle exception due to needing to get the original
    // exception on failure
    // CHECKSTYLE: stop IllegalCatch
    } catch (RuntimeException e1) {
    // CHECKSTYLE: resume IllegalCatch
      LOG.error("run: Worker failure failed on another RuntimeException, " +
          "original expection will be rethrown", e1);
    }
  }

  /**
   * Creates exception handler that will terminate process gracefully in case
   * of any uncaught exception.
   * @return new exception handler object.
   */
  public Thread.UncaughtExceptionHandler createUncaughtExceptionHandler() {
    return new OverrideExceptionHandler(
        CHECKER_IF_WORKER_SHOULD_FAIL_AFTER_EXCEPTION_CLASS.newInstance(
            getConf()), getJobProgressTracker());
  }

  public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
    return conf;
  }

  /**
   * @return Time spent in GC recorder by the GC listener
   */
  public long getSuperstepGCTime() {
    return (gcTimeMetric == null) ? 0 : gcTimeMetric.count();
  }

  /**
   * Returns a list of zookeeper servers to connect to.
   * If the port is set to 0 and Giraph is starting a single
   * ZooKeeper server, then Zookeeper will pick its own port.
   * Otherwise, the ZooKeeper port set by the user will be used.
   * @return host:port,host:port for each zookeeper
   */
  public String getZookeeperList() {
    if (zkManager != null) {
      return zkManager.getZooKeeperServerPortString();
    } else {
      return conf.getZookeeperList();
    }
  }

  /**
   * Default handler for uncaught exceptions.
   * It will do the best to clean up and then will terminate current giraph job.
   */
  class OverrideExceptionHandler implements Thread.UncaughtExceptionHandler {
    /** Checker if worker should fail after a thread gets an exception */
    private final CheckerIfWorkerShouldFailAfterException checker;
    /** JobProgressTracker to log problems to */
    private final JobProgressTracker jobProgressTracker;

    /**
     * Constructor
     *
     * @param checker Checker if worker should fail after a thread gets an
     *                exception
     * @param jobProgressTracker JobProgressTracker to log problems to
     */
    public OverrideExceptionHandler(
        CheckerIfWorkerShouldFailAfterException checker,
        JobProgressTracker jobProgressTracker) {
      this.checker = checker;
      this.jobProgressTracker = jobProgressTracker;
    }

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      if (!checker.checkIfWorkerShouldFail(t, e)) {
        return;
      }
      try {
        LOG.fatal(
            "uncaughtException: OverrideExceptionHandler on thread " +
                t.getName() + ", msg = " +  e.getMessage() + ", exiting...", e);
        byte [] exByteArray = KryoWritableWrapper.convertToByteArray(e);
        jobProgressTracker.logError(ExceptionUtils.getStackTrace(e),
                exByteArray);
        zooKeeperCleanup();
        workerFailureCleanup();
      } finally {
        System.exit(1);
      }
    }
  }

  /**
   * Interface to check if worker should fail after a thread gets an exception
   */
  public interface CheckerIfWorkerShouldFailAfterException {
    /**
     * Check if worker should fail after a thread gets an exception
     *
     * @param thread Thread which raised the exception
     * @param exception Exception which occurred
     * @return True iff worker should fail after this exception
     */
    boolean checkIfWorkerShouldFail(Thread thread, Throwable exception);
  }

  /**
   * Class to use by default, where each exception causes job failure
   */
  public static class FailWithEveryExceptionOccurred
      implements CheckerIfWorkerShouldFailAfterException {
    @Override
    public boolean checkIfWorkerShouldFail(Thread thread, Throwable exception) {
      return true;
    }
  }
}
