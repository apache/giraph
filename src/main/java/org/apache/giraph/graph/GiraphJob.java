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

import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.graph.partition.GraphPartitionerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Generates an appropriate internal {@link Job} for using Giraph in Hadoop.
 * Uses composition to avoid unwanted {@link Job} methods from exposure
 * to the user.
 */
public class GiraphJob {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Vertex class - required */
  public static final String VERTEX_CLASS = "giraph.vertexClass";
  /** VertexInputFormat class - required */
  public static final String VERTEX_INPUT_FORMAT_CLASS =
      "giraph.vertexInputFormatClass";

  /** Class for Master - optional */
  public static final String MASTER_COMPUTE_CLASS = "giraph.masterComputeClass";

  /** VertexOutputFormat class - optional */
  public static final String VERTEX_OUTPUT_FORMAT_CLASS =
      "giraph.vertexOutputFormatClass";
  /** Vertex combiner class - optional */
  public static final String VERTEX_COMBINER_CLASS =
      "giraph.combinerClass";
  /** Vertex resolver class - optional */
  public static final String VERTEX_RESOLVER_CLASS =
      "giraph.vertexResolverClass";
  /** Graph partitioner factory class - optional */
  public static final String GRAPH_PARTITIONER_FACTORY_CLASS =
      "giraph.graphPartitionerFactoryClass";

  /** Vertex index class */
  public static final String VERTEX_ID_CLASS = "giraph.vertexIdClass";
  /** Vertex value class */
  public static final String VERTEX_VALUE_CLASS = "giraph.vertexValueClass";
  /** Edge value class */
  public static final String EDGE_VALUE_CLASS = "giraph.edgeValueClass";
  /** Message value class */
  public static final String MESSAGE_VALUE_CLASS = "giraph.messageValueClass";
  /** Worker context class */
  public static final String WORKER_CONTEXT_CLASS =
      "giraph.workerContextClass";
  /** AggregatorWriter class - optional */
  public static final String AGGREGATOR_WRITER_CLASS =
      "giraph.aggregatorWriterClass";

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

  /** Indicates whether this job is run in an internal unit test */
  public static final String LOCAL_TEST_MODE =
      "giraph.localTestMode";

  /** not in local test mode per default */
  public static final boolean LOCAL_TEST_MODE_DEFAULT = false;

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
  public static final int POLL_MSECS_DEFAULT = 30 * 1000;

  /**
   *  ZooKeeper comma-separated list (if not set,
   *  will start up ZooKeeper locally)
   */
  public static final String ZOOKEEPER_LIST = "giraph.zkList";

  /** ZooKeeper session millisecond timeout */
  public static final String ZOOKEEPER_SESSION_TIMEOUT =
      "giraph.zkSessionMsecTimeout";
  /** Default Zookeeper session millisecond timeout */
  public static final int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 60 * 1000;

  /** Polling interval to check for the final ZooKeeper server data */
  public static final String ZOOKEEPER_SERVERLIST_POLL_MSECS =
      "giraph.zkServerlistPollMsecs";
  /** Default polling interval to check for the final ZooKeeper server data */
  public static final int ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT =
      3 * 1000;

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

  /** Use the RPC communication or netty communication */
  public static final String USE_NETTY = "giraph.useNetty";
  /** Default is to use RPC, not netty */
  public static final boolean USE_NETTY_DEFAULT = false;

  /** TCP backlog (defaults to number of workers) */
  public static final String TCP_BACKLOG = "giraph.tcpBacklog";
  /**
   * Default TCP backlog default if the number of workers is not specified
   * (i.e unittests)
   */
  public static final int TCP_BACKLOG_DEFAULT = 1;

  /** Netty simulate a first request closed */
  public static final String NETTY_SIMULATE_FIRST_REQUEST_CLOSED =
      "giraph.nettySimulateFirstRequestClosed";
  /** Default of not simulating failure for first request */
  public static final boolean NETTY_SIMULATE_FIRST_REQUEST_CLOSED_DEFAULT =
      false;

  /** Netty simulate a first response failed */
  public static final String NETTY_SIMULATE_FIRST_RESPONSE_FAILED =
      "giraph.nettySimulateFirstResponseFailed";
  /** Default of not simulating failure for first reponse */
  public static final boolean NETTY_SIMULATE_FIRST_RESPONSE_FAILED_DEFAULT =
      false;

  /** Maximum number of reconnection attempts */
  public static final String MAX_RECONNECT_ATTEMPTS =
      "giraph.maxNumberOfOpenRequests";
  /** Default maximum number of reconnection attempts */
  public static final int MAX_RECONNECT_ATTEMPTS_DEFAULT = 10;

  /** Max resolve address attempts */
  public static final String MAX_RESOLVE_ADDRESS_ATTEMPTS =
      "giraph.maxResolveAddressAttempts";
  /** Default max resolve address attempts */
  public static final int MAX_RESOLVE_ADDRESS_ATTEMPTS_DEFAULT = 5;

  /** Msecs to wait between waiting for all requests to finish */
  public static final String WAITING_REQUEST_MSECS =
      "giraph.waitingRequestMsecs";
  /** Default msecs to wait between waiting for all requests to finish */
  public static final int WAITING_REQUEST_MSECS_DEFAULT = 15000;

  /** Milliseconds for a request to complete (or else resend) */
  public static final String MAX_REQUEST_MILLISECONDS =
      "giraph.maxRequestMilliseconds";
  /** Maximum number of milliseconds for a request to complete */
  public static final int MAX_REQUEST_MILLISECONDS_DEFAULT = 60 * 1000;

  /** Netty max connection failures */
  public static final String NETTY_MAX_CONNECTION_FAILURES =
      "giraph.nettyMaxConnectionFailures";
  /** Default Netty max connection failures */
  public static final int NETTY_MAX_CONNECTION_FAILURES_DEFAULT = 1000;

  /** Initial port to start using for the RPC communication */
  public static final String RPC_INITIAL_PORT = "giraph.rpcInitialPort";
  /** Default port to start using for the RPC communication */
  public static final int RPC_INITIAL_PORT_DEFAULT = 30000;

  /** Maximum bind attempts for different RPC ports */
  public static final String MAX_RPC_PORT_BIND_ATTEMPTS =
      "giraph.maxRpcPortBindAttempts";
  /** Default maximum bind attempts for different RPC ports */
  public static final int MAX_RPC_PORT_BIND_ATTEMPTS_DEFAULT = 20;
  /**
   * Fail first RPC port binding attempt, simulate binding failure
   * on real grid testing
   */
  public static final String FAIL_FIRST_RPC_PORT_BIND_ATTEMPT =
      "giraph.failFirstRpcPortBindAttempt";
  /** Default fail first RPC port binding attempt flag */
  public static final boolean FAIL_FIRST_RPC_PORT_BIND_ATTEMPT_DEFAULT = false;

  /** Maximum number of RPC handlers */
  public static final String RPC_NUM_HANDLERS = "giraph.rpcNumHandlers";
  /** Default maximum number of RPC handlers */
  public static final int RPC_NUM_HANDLERS_DEFAULT = 100;

  /** Client send buffer size */
  public static final String CLIENT_SEND_BUFFER_SIZE =
      "giraph.clientSendBufferSize";
  /** Default client send buffer size of 0.5 MB */
  public static final int DEFAULT_CLIENT_SEND_BUFFER_SIZE = 512 * 1024;

  /** Client receive buffer size */
  public static final String CLIENT_RECEIVE_BUFFER_SIZE =
      "giraph.clientReceiveBufferSize";
  /** Default client receive buffer size of 32 k */
  public static final int DEFAULT_CLIENT_RECEIVE_BUFFER_SIZE = 32 * 1024;

  /** Server send buffer size */
  public static final String SERVER_SEND_BUFFER_SIZE =
      "giraph.serverSendBufferSize";
  /** Default server send buffer size of 32 k */
  public static final int DEFAULT_SERVER_SEND_BUFFER_SIZE = 32 * 1024;

  /** Server receive buffer size */
  public static final String SERVER_RECEIVE_BUFFER_SIZE =
      "giraph.serverReceiveBufferSize";
  /** Default server receive buffer size of 0.5 MB */
  public static final int DEFAULT_SERVER_RECEIVE_BUFFER_SIZE = 512 * 1024;

  /** Maximum number of messages per peer before flush */
  public static final String MSG_SIZE = "giraph.msgSize";
  /** Default maximum number of messages per peer before flush */
  public static final int MSG_SIZE_DEFAULT = 2000;

  /** Maximum number of mutations per partition before flush */
  public static final String MAX_MUTATIONS_PER_REQUEST =
      "giraph.maxMutationsPerRequest";
  /** Default maximum number of mutations per partition before flush */
  public static final int MAX_MUTATIONS_PER_REQUEST_DEFAULT = 100;

  /** Maximum number of messages that can be bulk sent during a flush */
  public static final String MAX_MESSAGES_PER_FLUSH_PUT =
      "giraph.maxMessagesPerFlushPut";
  /** Default number of messages that can be bulk sent during a flush */
  public static final int DEFAULT_MAX_MESSAGES_PER_FLUSH_PUT = 2000;

  /** Number of channels used per server */
  public static final String CHANNELS_PER_SERVER =
      "giraph.channelsPerServer";
  /** Default number of channels used per server of 1 */
  public static final int DEFAULT_CHANNELS_PER_SERVER = 1;

  /** Number of flush threads per peer */
  public static final String MSG_NUM_FLUSH_THREADS =
      "giraph.msgNumFlushThreads";

  /** Number of poll attempts prior to failing the job (int) */
  public static final String POLL_ATTEMPTS = "giraph.pollAttempts";
  /** Default poll attempts */
  public static final int POLL_ATTEMPTS_DEFAULT = 10;

  /** Number of minimum vertices in each vertex range */
  public static final String MIN_VERTICES_PER_RANGE =
      "giraph.minVerticesPerRange";
  /** Default number of minimum vertices in each vertex range */
  public static final long MIN_VERTICES_PER_RANGE_DEFAULT = 3;

  /** Minimum stragglers of the superstep before printing them out */
  public static final String PARTITION_LONG_TAIL_MIN_PRINT =
      "giraph.partitionLongTailMinPrint";
  /** Only print stragglers with one as a default */
  public static final int PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT = 1;

  /** Use superstep counters? (boolean) */
  public static final String USE_SUPERSTEP_COUNTERS =
      "giraph.useSuperstepCounters";
  /** Default is to use the superstep counters */
  public static final boolean USE_SUPERSTEP_COUNTERS_DEFAULT = true;

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

  /**
   * Input split sample percent - Used only for sampling and testing, rather
   * than an actual job.  The idea is that to test, you might only want a
   * fraction of the actual input splits from your VertexInputFormat to
   * load (values should be [0, 100]).
   */
  public static final String INPUT_SPLIT_SAMPLE_PERCENT =
      "giraph.inputSplitSamplePercent";
  /** Default is to use all the input splits */
  public static final float INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT = 100f;

  /**
   * To limit outlier input splits from producing too many vertices or to
   * help with testing, the number of vertices loaded from an input split can
   * be limited.  By default, everything is loaded.
   */
  public static final String INPUT_SPLIT_MAX_VERTICES =
      "giraph.InputSplitMaxVertices";
  /**
   * Default is that all the vertices are to be loaded from the input
   * split
   */
  public static final long INPUT_SPLIT_MAX_VERTICES_DEFAULT = -1;

  /** Java opts passed to ZooKeeper startup */
  public static final String ZOOKEEPER_JAVA_OPTS =
      "giraph.zkJavaOpts";
  /** Default java opts passed to ZooKeeper startup */
  public static final String ZOOKEEPER_JAVA_OPTS_DEFAULT =
      "-Xmx512m -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC " +
          "-XX:CMSInitiatingOccupancyFraction=70 -XX:MaxGCPauseMillis=100";

  /**
   *  How often to checkpoint (i.e. 0, means no checkpoint,
   *  1 means every superstep, 2 is every two supersteps, etc.).
   */
  public static final String CHECKPOINT_FREQUENCY =
      "giraph.checkpointFrequency";

  /** Default checkpointing frequency of none. */
  public static final int CHECKPOINT_FREQUENCY_DEFAULT = 0;

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
   * Base ZNode for Giraph's state in the ZooKeeper cluster.  Must be a root
   * znode on the cluster beginning with "/"
   */
  public static final String BASE_ZNODE_KEY = "giraph.zkBaseZNode";

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

  /** Directory in the local file system for out-of-core messages. */
  public static final String MESSAGES_DIRECTORY = "giraph.messagesDirectory";
  /**
   * Default messages directory. Final directory path will also have the
   * job number for uniqueness
   */
  public static final String MESSAGES_DIRECTORY_DEFAULT = "_bsp/_messages/";

  /** Whether or not to use out-of-core messages */
  public static final String USE_OUT_OF_CORE_MESSAGES =
      "giraph.useOutOfCoreMessages";
  /** Default choice about using out-of-core messaging */
  public static final boolean USE_OUT_OF_CORE_MESSAGES_DEFAULT = false;
  /**
   * If using out-of-core messaging, it tells how much messages do we keep
   * in memory.
   */
  public static final String MAX_MESSAGES_IN_MEMORY =
      "giraph.maxMessagesInMemory";
  /** Default maximum number of messages in memory. */
  public static final int MAX_MESSAGES_IN_MEMORY_DEFAULT = 1000000;
  /** Size of buffer when reading and writing messages out-of-core. */
  public static final String MESSAGES_BUFFER_SIZE =
      "giraph.messagesBufferSize";
  /** Default size of buffer when reading and writing messages out-of-core. */
  public static final int MESSAGES_BUFFER_SIZE_DEFAULT = 8192;

  /** Directory in the local filesystem for out-of-core partitions. */
  public static final String PARTITIONS_DIRECTORY =
      "giraph.partitionsDirectory";
  /** Default directory for out-of-core partitions. */
  public static final String PARTITIONS_DIRECTORY_DEFAULT = "_bsp/_partitions";

  /** Enable out-of-core graph. */
  public static final String USE_OUT_OF_CORE_GRAPH =
      "giraph.useOutOfCoreGraph";
  /** Default is not to use out-of-core graph. */
  public static final boolean USE_OUT_OF_CORE_GRAPH_DEFAULT = false;

  /** Maximum number of partitions to hold in memory for each worker. */
  public static final String MAX_PARTITIONS_IN_MEMORY =
      "giraph.maxPartitionsInMemory";
  /** Default maximum number of in-memory partitions. */
  public static final int MAX_PARTITIONS_IN_MEMORY_DEFAULT = 10;

  /** Keep the zookeeper output for debugging? Default is to remove it. */
  public static final String KEEP_ZOOKEEPER_DATA =
      "giraph.keepZooKeeperData";
  /** Default is to remove ZooKeeper data. */
  public static final Boolean KEEP_ZOOKEEPER_DATA_DEFAULT = false;

  /** Default ZooKeeper tick time. */
  public static final int DEFAULT_ZOOKEEPER_TICK_TIME = 6000;
  /** Default ZooKeeper init limit (in ticks). */
  public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;
  /** Default ZooKeeper sync limit (in ticks). */
  public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;
  /** Default ZooKeeper snap count. */
  public static final int DEFAULT_ZOOKEEPER_SNAP_COUNT = 50000;
  /** Default ZooKeeper maximum client connections. */
  public static final int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 10000;
  /** Default ZooKeeper minimum session timeout of 5 minutes (in msecs). */
  public static final int DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT = 300 * 1000;
  /** Default ZooKeeper maximum session timeout of 10 minutes (in msecs). */
  public static final int DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT = 600 * 1000;

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphJob.class);

  /** Internal job that actually is submitted */
  private final Job job;
  /** Helper configuration from the job */
  private final Configuration conf;


  /**
   * Constructor that will instantiate the configuration
   *
   * @param jobName User-defined job name
   * @throws IOException
   */
  public GiraphJob(String jobName) throws IOException {
    this(new Configuration(), jobName);
  }

  /**
   * Constructor.
   *
   * @param conf User-defined configuration
   * @param jobName User-defined job name
   * @throws IOException
   */
  public GiraphJob(Configuration conf, String jobName) throws IOException {
    job = new Job(conf, jobName);
    this.conf = job.getConfiguration();
  }

  /**
   * Get the configuration from the internal job.
   *
   * @return Configuration used by the job.
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Be very cautious when using this method as it returns the internal job
   * of {@link GiraphJob}.  This should only be used for methods that require
   * access to the actual {@link Job}, i.e. FileInputFormat#addInputPath().
   *
   * @return Internal job that will actually be submitted to Hadoop.
   */
  public Job getInternalJob() {
    return job;
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
   * Set the vertex class (required)
   *
   * @param vertexClass Runs vertex computation
   */
  public final void setVertexClass(Class<?> vertexClass) {
    getConfiguration().setClass(VERTEX_CLASS, vertexClass, Vertex.class);
  }

  /**
   * Set the vertex input format class (required)
   *
   * @param vertexInputFormatClass Determines how graph is input
   */
  public final void setVertexInputFormatClass(
      Class<?> vertexInputFormatClass) {
    getConfiguration().setClass(VERTEX_INPUT_FORMAT_CLASS,
        vertexInputFormatClass,
        VertexInputFormat.class);
  }

  /**
   * Set the master class (optional)
   *
   * @param masterComputeClass Runs master computation
   */
  public final void setMasterComputeClass(Class<?> masterComputeClass) {
    getConfiguration().setClass(MASTER_COMPUTE_CLASS, masterComputeClass,
        MasterCompute.class);
  }

  /**
   * Set the vertex output format class (optional)
   *
   * @param vertexOutputFormatClass Determines how graph is output
   */
  public final void setVertexOutputFormatClass(
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
  public final void setVertexCombinerClass(Class<?> vertexCombinerClass) {
    getConfiguration().setClass(VERTEX_COMBINER_CLASS,
        vertexCombinerClass,
        VertexCombiner.class);
  }

  /**
   * Set the graph partitioner class (optional)
   *
   * @param graphPartitionerFactoryClass Determines how the graph is partitioned
   */
  public final void setGraphPartitionerFactoryClass(
      Class<?> graphPartitionerFactoryClass) {
    getConfiguration().setClass(GRAPH_PARTITIONER_FACTORY_CLASS,
        graphPartitionerFactoryClass,
        GraphPartitionerFactory.class);
  }

  /**
   * Set the vertex resolver class (optional)
   *
   * @param vertexResolverClass Determines how vertex mutations are resolved
   */
  public final void setVertexResolverClass(Class<?> vertexResolverClass) {
    getConfiguration().setClass(VERTEX_RESOLVER_CLASS,
        vertexResolverClass,
        VertexResolver.class);
  }

  /**
   * Set the worker context class (optional)
   *
   * @param workerContextClass Determines what code is executed on a each
   *        worker before and after each superstep and computation
   */
  public final void setWorkerContextClass(Class<?> workerContextClass) {
    getConfiguration().setClass(WORKER_CONTEXT_CLASS,
        workerContextClass,
        WorkerContext.class);
  }

  /**
   * Set the aggregator writer class (optional)
   *
   * @param aggregatorWriterClass Determines how the aggregators are
   *        written to file at the end of the job
   */
  public final void setAggregatorWriterClass(
      Class<?> aggregatorWriterClass) {
    getConfiguration().setClass(AGGREGATOR_WRITER_CLASS,
        aggregatorWriterClass,
        AggregatorWriter.class);
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
  public final void setWorkerConfiguration(int minWorkers,
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
  public final void setZooKeeperConfiguration(String serverList) {
    conf.set(ZOOKEEPER_LIST, serverList);
  }

  /**
   * Check if the configuration is local.  If it is local, do additional
   * checks due to the restrictions of LocalJobRunner.
   *
   * @param conf Configuration
   */
  private static void checkLocalJobRunnerConfiguration(
      Configuration conf) {
    String jobTracker = conf.get("mapred.job.tracker", null);
    if (!jobTracker.equals("local")) {
      // Nothing to check
      return;
    }

    int maxWorkers = conf.getInt(MAX_WORKERS, -1);
    if (maxWorkers != 1) {
      throw new IllegalArgumentException(
          "checkLocalJobRunnerConfiguration: When using " +
              "LocalJobRunner, must have only one worker since " +
          "only 1 task at a time!");
    }
    if (conf.getBoolean(SPLIT_MASTER_WORKER,
        SPLIT_MASTER_WORKER_DEFAULT)) {
      throw new IllegalArgumentException(
          "checkLocalJobRunnerConfiguration: When using " +
              "LocalJobRunner, you cannot run in split master / worker " +
          "mode since there is only 1 task at a time!");
    }
  }

  /**
   * Check whether a specified int conf value is set and if not, set it.
   *
   * @param param Conf value to check
   * @param defaultValue Assign to value if not set
   */
  private void setIntConfIfDefault(String param, int defaultValue) {
    if (conf.getInt(param, Integer.MIN_VALUE) == Integer.MIN_VALUE) {
      conf.setInt(param, defaultValue);
    }
  }

  /**
   * Runs the actual graph application through Hadoop Map-Reduce.
   *
   * @param verbose If true, provide verbose output, false otherwise
   * @return True if success, false otherwise
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  public final boolean run(boolean verbose)
    throws IOException, InterruptedException, ClassNotFoundException {
    checkConfiguration();
    checkLocalJobRunnerConfiguration(conf);
    job.setNumReduceTasks(0);
    // Most users won't hit this hopefully and can set it higher if desired
    setIntConfIfDefault("mapreduce.job.counters.limit", 512);

    // Capacity scheduler-specific settings.  These should be enough for
    // a reasonable Giraph job
    setIntConfIfDefault("mapred.job.map.memory.mb", 1024);
    setIntConfIfDefault("mapred.job.reduce.memory.mb", 1024);

    // Speculative execution doesn't make sense for Giraph
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);

    // Set the ping interval to 5 minutes instead of one minute
    // (DEFAULT_PING_INTERVAL)
    Client.setPingInterval(conf, 60000 * 5);

    if (job.getJar() == null) {
      job.setJarByClass(GiraphJob.class);
    }
    // Should work in MAPREDUCE-1938 to let the user jars/classes
    // get loaded first
    conf.setBoolean("mapreduce.user.classpath.first", true);

    job.setMapperClass(GraphMapper.class);
    job.setInputFormatClass(BspInputFormat.class);
    job.setOutputFormatClass(BspOutputFormat.class);
    return job.waitForCompletion(verbose);
  }
}
