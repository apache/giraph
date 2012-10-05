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

package org.apache.giraph;

import org.apache.giraph.graph.AggregatorWriter;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.GraphPartitionerFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Adds user methods specific to Giraph.  This will be put into an
 * ImmutableClassesGiraphConfiguration that provides the configuration plus
 * the immutable classes.
 */
public class GiraphConfiguration extends Configuration {
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

  /** Override the Hadoop log level and set the desired log level. */
  public static final String LOG_LEVEL = "giraph.logLevel";
  /** Default log level is INFO (same as Hadoop) */
  public static final String LOG_LEVEL_DEFAULT = "info";

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

  /** How big to make the default buffer? */
  public static final String NETTY_REQUEST_ENCODER_BUFFER_SIZE =
      "giraph.nettyRequestEncoderBufferSize";
  /** Start with 32K */
  public static final int NETTY_REQUEST_ENCODER_BUFFER_SIZE_DEFAULT =
      32 * 1024;

  /** Netty client threads */
  public static final String NETTY_CLIENT_THREADS =
      "giraph.nettyClientThreads";
  /** Default is 4 */
  public static final int NETTY_CLIENT_THREADS_DEFAULT = 4;

  /** Netty server threads */
  public static final String NETTY_SERVER_THREADS =
      "giraph.nettyServerThreads";
  /** Default is 16 */
  public static final int NETTY_SERVER_THREADS_DEFAULT = 16;

  /** Use the execution handler in netty on the client? */
  public static final String NETTY_CLIENT_USE_EXECUTION_HANDLER =
      "giraph.nettyClientUseExecutionHandler";
  /** Use the execution handler in netty on the client - default true */
  public static final boolean NETTY_CLIENT_USE_EXECUTION_HANDLER_DEFAULT =
      true;

  /** Netty client execution threads (execution handler) */
  public static final String NETTY_CLIENT_EXECUTION_THREADS =
      "giraph.nettyClientExecutionThreads";
  /** Default Netty client execution threads (execution handler) of 8 */
  public static final int NETTY_CLIENT_EXECUTION_THREADS_DEFAULT = 8;

  /** Where to place the netty client execution handle? */
  public static final String NETTY_CLIENT_EXECUTION_AFTER_HANDLER =
      "giraph.nettyClientExecutionAfterHandler";
  /**
   * Default is to use the netty client execution handle after the request
   * encoder.
   */
  public static final String NETTY_CLIENT_EXECUTION_AFTER_HANDLER_DEFAULT =
      "requestEncoder";

  /** Use the execution handler in netty on the server? */
  public static final String NETTY_SERVER_USE_EXECUTION_HANDLER =
      "giraph.nettyServerUseExecutionHandler";
  /** Use the execution handler in netty on the server - default true */
  public static final boolean NETTY_SERVER_USE_EXECUTION_HANDLER_DEFAULT =
      true;

  /** Netty server execution threads (execution handler) */
  public static final String NETTY_SERVER_EXECUTION_THREADS =
      "giraph.nettyServerExecutionThreads";
  /** Default Netty server execution threads (execution handler) of 8 */
  public static final int NETTY_SERVER_EXECUTION_THREADS_DEFAULT = 8;

  /** Where to place the netty server execution handle? */
  public static final String NETTY_SERVER_EXECUTION_AFTER_HANDLER =
      "giraph.nettyServerExecutionAfterHandler";
  /**
   * Default is to use the netty server execution handle after the request
   * frame decoder.
   */
  public static final String NETTY_SERVER_EXECUTION_AFTER_HANDLER_DEFAULT =
      "requestFrameDecoder";

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
  /** Maximum number of milliseconds for a request to complete (10 minutes) */
  public static final int MAX_REQUEST_MILLISECONDS_DEFAULT = 10 * 60 * 1000;

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

  /** Number of ZooKeeper client connection attempts before giving up. */
  public static final String ZOOKEEPER_CONNECTION_ATTEMPTS =
      "giraph.zkConnectionAttempts";
  /** Default of 10 ZooKeeper client connection attempts before giving up. */
  public static final int ZOOKEEPER_CONNECTION_ATTEMPTS_DEFAULT = 10;

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
  /** ZooKeeper minimum session timeout */
  public static final String ZOOKEEPER_MIN_SESSION_TIMEOUT =
      "giraph.zKMinSessionTimeout";
  /** Default ZooKeeper minimum session timeout of 10 minutes (in msecs). */
  public static final int DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT = 600 * 1000;
  /** ZooKeeper maximum session timeout */
  public static final String ZOOKEEPER_MAX_SESSION_TIMEOUT =
      "giraph.zkMaxSessionTimeout";
  /** Default ZooKeeper maximum session timeout of 15 minutes (in msecs). */
  public static final int DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT = 900 * 1000;
  /** ZooKeeper force sync */
  public static final String ZOOKEEPER_FORCE_SYNC = "giraph.zKForceSync";
  /** Default ZooKeeper force sync is off (for performance) */
  public static final String DEFAULT_ZOOKEEPER_FORCE_SYNC = "no";
  /** ZooKeeper skip ACLs */
  public static final String ZOOKEEPER_SKIP_ACL = "giraph.ZkSkipAcl";
  /** Default ZooKeeper skip ACLs true (for performance) */
  public static final String DEFAULT_ZOOKEEPER_SKIP_ACL = "yes";

  /**
   * Constructor that creates the configuration
   */
  public GiraphConfiguration() { }

  /**
   * Constructor.
   *
   * @param conf Configuration
   */
  public GiraphConfiguration(Configuration conf) {
    super(conf);
  }

  /**
   * Set the vertex class (required)
   *
   * @param vertexClass Runs vertex computation
   */
  public final void setVertexClass(
      Class<? extends Vertex> vertexClass) {
    setClass(VERTEX_CLASS, vertexClass, Vertex.class);
  }

  /**
   * Set the vertex input format class (required)
   *
   * @param vertexInputFormatClass Determines how graph is input
   */
  public final void setVertexInputFormatClass(
      Class<? extends VertexInputFormat> vertexInputFormatClass) {
    setClass(VERTEX_INPUT_FORMAT_CLASS,
        vertexInputFormatClass,
        VertexInputFormat.class);
  }

  /**
   * Set the master class (optional)
   *
   * @param masterComputeClass Runs master computation
   */
  public final void setMasterComputeClass(
      Class<? extends MasterCompute> masterComputeClass) {
    setClass(MASTER_COMPUTE_CLASS, masterComputeClass,
        MasterCompute.class);
  }

  /**
   * Set the vertex output format class (optional)
   *
   * @param vertexOutputFormatClass Determines how graph is output
   */
  public final void setVertexOutputFormatClass(
      Class<? extends VertexOutputFormat> vertexOutputFormatClass) {
    setClass(VERTEX_OUTPUT_FORMAT_CLASS,
        vertexOutputFormatClass,
        VertexOutputFormat.class);
  }

  /**
   * Set the vertex combiner class (optional)
   *
   * @param vertexCombinerClass Determines how vertex messages are combined
   */
  public final void setVertexCombinerClass(
      Class<? extends VertexCombiner> vertexCombinerClass) {
    setClass(VERTEX_COMBINER_CLASS,
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
    setClass(GRAPH_PARTITIONER_FACTORY_CLASS,
        graphPartitionerFactoryClass,
        GraphPartitionerFactory.class);
  }

  /**
   * Set the vertex resolver class (optional)
   *
   * @param vertexResolverClass Determines how vertex mutations are resolved
   */
  public final void setVertexResolverClass(
      Class<? extends VertexResolver> vertexResolverClass) {
    setClass(VERTEX_RESOLVER_CLASS,
        vertexResolverClass,
        VertexResolver.class);
  }

  /**
   * Set the worker context class (optional)
   *
   * @param workerContextClass Determines what code is executed on a each
   *        worker before and after each superstep and computation
   */
  public final void setWorkerContextClass(
      Class<? extends WorkerContext> workerContextClass) {
    setClass(WORKER_CONTEXT_CLASS,
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
      Class<? extends AggregatorWriter> aggregatorWriterClass) {
    setClass(AGGREGATOR_WRITER_CLASS,
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
    setInt(MIN_WORKERS, minWorkers);
    setInt(MAX_WORKERS, maxWorkers);
    setFloat(MIN_PERCENT_RESPONDED, minPercentResponded);
  }

  public final int getMinWorkers() {
    return getInt(MIN_WORKERS, -1);
  }

  public final int getMaxWorkers() {
    return getInt(MAX_WORKERS, -1);
  }

  public final float getMinPercentResponded() {
    return getFloat(MIN_PERCENT_RESPONDED, MIN_PERCENT_RESPONDED_DEFAULT);
  }

  /**
   * Utilize an existing ZooKeeper service.  If this is not set, ZooKeeper
   * will be dynamically started by Giraph for this job.
   *
   * @param serverList Comma separated list of servers and ports
   *        (i.e. zk1:2221,zk2:2221)
   */
  public final void setZooKeeperConfiguration(String serverList) {
    set(ZOOKEEPER_LIST, serverList);
  }

  public final boolean getSplitMasterWorker() {
    return getBoolean(SPLIT_MASTER_WORKER, SPLIT_MASTER_WORKER_DEFAULT);
  }

  /**
   * Get the task partition
   *
   * @return The task partition or -1 if not set
   */
  public int getTaskPartition() {
    return getInt("mapred.task.partition", -1);
  }

  /**
   * Get the ZooKeeper list.
   *
   * @return ZooKeeper list of strings, comma separated or null if none set.
   */
  public String getZookeeperList() {
    return get(ZOOKEEPER_LIST);
  }

  public String getLocalLevel() {
    return get(LOG_LEVEL, LOG_LEVEL_DEFAULT);
  }

  public boolean getLocalTestMode() {
    return getBoolean(LOCAL_TEST_MODE, LOCAL_TEST_MODE_DEFAULT);
  }

  public boolean getUseNetty() {
    return getBoolean(USE_NETTY, USE_NETTY_DEFAULT);
  }

  public int getZooKeeperServerCount() {
    return getInt(ZOOKEEPER_SERVER_COUNT,
        ZOOKEEPER_SERVER_COUNT_DEFAULT);
  }

  /**
   * Set the ZooKeeper jar classpath
   *
   * @param classPath Classpath for the ZooKeeper jar
   */
  public void setZooKeeperJar(String classPath) {
    set(ZOOKEEPER_JAR, classPath);
  }

  public int getZooKeeperSessionTimeout() {
    return getInt(ZOOKEEPER_SESSION_TIMEOUT,
        ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
  }

  public boolean getNettyServerUseExecutionHandler() {
    return getBoolean(NETTY_SERVER_USE_EXECUTION_HANDLER,
        NETTY_SERVER_USE_EXECUTION_HANDLER_DEFAULT);
  }

  public int getNettyServerThreads() {
    return getInt(NETTY_SERVER_THREADS, NETTY_SERVER_THREADS_DEFAULT);
  }

  public int getNettyServerExecutionThreads() {
    return getInt(NETTY_SERVER_EXECUTION_THREADS,
        NETTY_SERVER_EXECUTION_THREADS_DEFAULT);
  }

  /**
   * Get the netty server execution concurrency.  This depends on whether the
   * netty server execution handler exists.
   *
   * @return Server concurrency
   */
  public int getNettyServerExecutionConcurrency() {
    if (getNettyServerUseExecutionHandler()) {
      return getNettyServerExecutionThreads();
    } else {
      return getNettyServerThreads();
    }
  }

  public int getZookeeperConnectionAttempts() {
    return getInt(ZOOKEEPER_CONNECTION_ATTEMPTS,
                  ZOOKEEPER_CONNECTION_ATTEMPTS_DEFAULT);
  }

  public int getZooKeeperMinSessionTimeout() {
    return getInt(ZOOKEEPER_MIN_SESSION_TIMEOUT,
        DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT);
  }

  public int getZooKeeperMaxSessionTimeout() {
    return getInt(ZOOKEEPER_MAX_SESSION_TIMEOUT,
        DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT);
  }

  public String getZooKeeperForceSync() {
    return get(ZOOKEEPER_FORCE_SYNC, DEFAULT_ZOOKEEPER_FORCE_SYNC);
  }

  public String getZooKeeperSkipAcl() {
    return get(ZOOKEEPER_SKIP_ACL, DEFAULT_ZOOKEEPER_SKIP_ACL);
  }

  /**
   * Get the number of map tasks in this job
   *
   * @return Number of map tasks in this job
   */
  public int getMapTasks() {
    int mapTasks = getInt("mapred.map.tasks", -1);
    if (mapTasks == -1) {
      throw new IllegalStateException("getMapTasks: Failed to get the map " +
          "tasks!");
    }
    return mapTasks;
  }
}
