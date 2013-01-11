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
package org.apache.giraph.conf;

/**
 * Constants used all over Giraph for configuration.
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface GiraphConstants {
  /** Vertex class - required */
  String VERTEX_CLASS = "giraph.vertexClass";

  /** Class for Master - optional */
  String MASTER_COMPUTE_CLASS = "giraph.masterComputeClass";
  /** Classes for Observer Master - optional */
  String MASTER_OBSERVER_CLASSES = "giraph.master.observers";
  /** Vertex combiner class - optional */
  String VERTEX_COMBINER_CLASS = "giraph.combinerClass";
  /** Vertex resolver class - optional */
  String VERTEX_RESOLVER_CLASS = "giraph.vertexResolverClass";
  /**
   * Option of whether to create vertexes that were not existent before but
   * received messages
   */
  String RESOLVER_CREATE_VERTEX_ON_MSGS =
      "giraph.vertex.resolver.create.on.msgs";
  /** Graph partitioner factory class - optional */
  String GRAPH_PARTITIONER_FACTORY_CLASS =
      "giraph.graphPartitionerFactoryClass";

  // At least one of the input format classes is required.
  /** VertexInputFormat class */
  String VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
  /** EdgeInputFormat class */
  String EDGE_INPUT_FORMAT_CLASS = "giraph.edgeInputFormatClass";

  /** VertexOutputFormat class */
  String VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";

  /** Vertex index class */
  String VERTEX_ID_CLASS = "giraph.vertexIdClass";
  /** Vertex value class */
  String VERTEX_VALUE_CLASS = "giraph.vertexValueClass";
  /** Edge value class */
  String EDGE_VALUE_CLASS = "giraph.edgeValueClass";
  /** Message value class */
  String MESSAGE_VALUE_CLASS = "giraph.messageValueClass";
  /** Worker context class */
  String WORKER_CONTEXT_CLASS = "giraph.workerContextClass";
  /** AggregatorWriter class - optional */
  String AGGREGATOR_WRITER_CLASS = "giraph.aggregatorWriterClass";

  /** Partition class - optional */
  String PARTITION_CLASS = "giraph.partitionClass";

  /**
   * Minimum number of simultaneous workers before this job can run (int)
   */
  String MIN_WORKERS = "giraph.minWorkers";
  /**
   * Maximum number of simultaneous worker tasks started by this job (int).
   */
  String MAX_WORKERS = "giraph.maxWorkers";

  /**
   * Separate the workers and the master tasks.  This is required
   * to support dynamic recovery. (boolean)
   */
  String SPLIT_MASTER_WORKER = "giraph.SplitMasterWorker";
  /**
   * Default on whether to separate the workers and the master tasks.
   * Needs to be "true" to support dynamic recovery.
   */
  boolean SPLIT_MASTER_WORKER_DEFAULT = true;

  /** Indicates whether this job is run in an internal unit test */
  String LOCAL_TEST_MODE = "giraph.localTestMode";

  /** not in local test mode per default */
  boolean LOCAL_TEST_MODE_DEFAULT = false;

  /** Override the Hadoop log level and set the desired log level. */
  String LOG_LEVEL = "giraph.logLevel";
  /** Default log level is INFO (same as Hadoop) */
  String LOG_LEVEL_DEFAULT = "info";

  /** Use thread level debugging? */
  String LOG_THREAD_LAYOUT = "giraph.logThreadLayout";
  /** Default to not use thread-level debugging */
  boolean LOG_THREAD_LAYOUT_DEFAULT = false;

  /**
   * Minimum percent of the maximum number of workers that have responded
   * in order to continue progressing. (float)
   */
  String MIN_PERCENT_RESPONDED = "giraph.minPercentResponded";
  /** Default 100% response rate for workers */
  float MIN_PERCENT_RESPONDED_DEFAULT = 100.0f;

  /** Enable the Metrics system **/
  String METRICS_ENABLE = "giraph.metrics.enable";

  /**
   *  ZooKeeper comma-separated list (if not set,
   *  will start up ZooKeeper locally)
   */
  String ZOOKEEPER_LIST = "giraph.zkList";

  /** ZooKeeper session millisecond timeout */
  String ZOOKEEPER_SESSION_TIMEOUT = "giraph.zkSessionMsecTimeout";
  /** Default Zookeeper session millisecond timeout */
  int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 60 * 1000;

  /** Polling interval to check for the ZooKeeper server data */
  String ZOOKEEPER_SERVERLIST_POLL_MSECS = "giraph.zkServerlistPollMsecs";
  /** Default polling interval to check for the ZooKeeper server data */
  int ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT = 3 * 1000;

  /** Number of nodes (not tasks) to run Zookeeper on */
  String ZOOKEEPER_SERVER_COUNT = "giraph.zkServerCount";
  /** Default number of nodes to run Zookeeper on */
  int ZOOKEEPER_SERVER_COUNT_DEFAULT = 1;

  /** ZooKeeper port to use */
  String ZOOKEEPER_SERVER_PORT = "giraph.zkServerPort";
  /** Default ZooKeeper port to use */
  int ZOOKEEPER_SERVER_PORT_DEFAULT = 22181;

  /** Location of the ZooKeeper jar - Used internally, not meant for users */
  String ZOOKEEPER_JAR = "giraph.zkJar";

  /** Local ZooKeeper directory to use */
  String ZOOKEEPER_DIR = "giraph.zkDir";

  /** Max attempts for handling ZooKeeper connection loss */
  String ZOOKEEPER_OPS_MAX_ATTEMPTS = "giraph.zkOpsMaxAttempts";
  /** Default of 3 attempts for handling ZooKeeper connection loss */
  int ZOOKEEPER_OPS_MAX_ATTEMPTS_DEFAULT = 3;

  /**
   * Msecs to wait before retrying a failed ZooKeeper op due to connection
   * loss.
   */
  String ZOOKEEPER_OPS_RETRY_WAIT_MSECS = "giraph.zkOpsRetryWaitMsecs";
  /**
   * Default to wait 5 seconds before retrying a failed ZooKeeper op due to
   * connection loss.
   */
  int ZOOKEEPER_OPS_RETRY_WAIT_MSECS_DEFAULT = 5 * 1000;

  /** TCP backlog (defaults to number of workers) */
  String TCP_BACKLOG = "giraph.tcpBacklog";
  /**
   * Default TCP backlog default if the number of workers is not specified
   * (i.e unittests)
   */
  int TCP_BACKLOG_DEFAULT = 1;

  /** How big to make the default buffer? */
  String NETTY_REQUEST_ENCODER_BUFFER_SIZE =
      "giraph.nettyRequestEncoderBufferSize";
  /** Start with 32K */
  int NETTY_REQUEST_ENCODER_BUFFER_SIZE_DEFAULT = 32 * 1024;

  /** Whether or not netty request encoder should use direct byte buffers */
  String NETTY_REQUEST_ENCODER_USE_DIRECT_BUFFERS =
      "giraph.nettyRequestEncoderUseDirectBuffers";
  /**
   * By default don't use direct buffers,
   * since jobs can take more than allowed heap memory in that case
   */
  boolean NETTY_REQUEST_ENCODER_USE_DIRECT_BUFFERS_DEFAULT = false;

  /** Netty client threads */
  String NETTY_CLIENT_THREADS = "giraph.nettyClientThreads";
  /** Default is 4 */
  int NETTY_CLIENT_THREADS_DEFAULT = 4;

  /** Netty server threads */
  String NETTY_SERVER_THREADS = "giraph.nettyServerThreads";
  /** Default is 16 */
  int NETTY_SERVER_THREADS_DEFAULT = 16;

  /** Use the execution handler in netty on the client? */
  String NETTY_CLIENT_USE_EXECUTION_HANDLER =
      "giraph.nettyClientUseExecutionHandler";
  /** Use the execution handler in netty on the client - default true */
  boolean NETTY_CLIENT_USE_EXECUTION_HANDLER_DEFAULT = true;

  /** Netty client execution threads (execution handler) */
  String NETTY_CLIENT_EXECUTION_THREADS =
      "giraph.nettyClientExecutionThreads";
  /** Default Netty client execution threads (execution handler) of 8 */
  int NETTY_CLIENT_EXECUTION_THREADS_DEFAULT = 8;

  /** Where to place the netty client execution handle? */
  String NETTY_CLIENT_EXECUTION_AFTER_HANDLER =
      "giraph.nettyClientExecutionAfterHandler";
  /**
   * Default is to use the netty client execution handle after the request
   * encoder.
   */
  String NETTY_CLIENT_EXECUTION_AFTER_HANDLER_DEFAULT = "requestEncoder";

  /** Use the execution handler in netty on the server? */
  String NETTY_SERVER_USE_EXECUTION_HANDLER =
      "giraph.nettyServerUseExecutionHandler";
  /** Use the execution handler in netty on the server - default true */
  boolean NETTY_SERVER_USE_EXECUTION_HANDLER_DEFAULT = true;

  /** Netty server execution threads (execution handler) */
  String NETTY_SERVER_EXECUTION_THREADS = "giraph.nettyServerExecutionThreads";
  /** Default Netty server execution threads (execution handler) of 8 */
  int NETTY_SERVER_EXECUTION_THREADS_DEFAULT = 8;

  /** Where to place the netty server execution handle? */
  String NETTY_SERVER_EXECUTION_AFTER_HANDLER =
      "giraph.nettyServerExecutionAfterHandler";
  /**
   * Default is to use the netty server execution handle after the request
   * frame decoder.
   */
  String NETTY_SERVER_EXECUTION_AFTER_HANDLER_DEFAULT = "requestFrameDecoder";

  /** Netty simulate a first request closed */
  String NETTY_SIMULATE_FIRST_REQUEST_CLOSED =
      "giraph.nettySimulateFirstRequestClosed";
  /** Default of not simulating failure for first request */
  boolean NETTY_SIMULATE_FIRST_REQUEST_CLOSED_DEFAULT = false;

  /** Netty simulate a first response failed */
  String NETTY_SIMULATE_FIRST_RESPONSE_FAILED =
      "giraph.nettySimulateFirstResponseFailed";
  /** Default of not simulating failure for first reponse */
  boolean NETTY_SIMULATE_FIRST_RESPONSE_FAILED_DEFAULT = false;

  /** Max resolve address attempts */
  String MAX_RESOLVE_ADDRESS_ATTEMPTS = "giraph.maxResolveAddressAttempts";
  /** Default max resolve address attempts */
  int MAX_RESOLVE_ADDRESS_ATTEMPTS_DEFAULT = 5;

  /** Msecs to wait between waiting for all requests to finish */
  String WAITING_REQUEST_MSECS = "giraph.waitingRequestMsecs";
  /** Default msecs to wait between waiting for all requests to finish */
  int WAITING_REQUEST_MSECS_DEFAULT = 15000;

  /** Millseconds to wait for an event before continuing */
  String EVENT_WAIT_MSECS = "giraph.eventWaitMsecs";
  /**
   * Default milliseconds to wait for an event before continuing (30 seconds)
   */
  int EVENT_WAIT_MSECS_DEFAULT = 30 * 1000;

  /**
   * Maximum milliseconds to wait before giving up trying to get the minimum
   * number of workers before a superstep (int).
   */
  String MAX_MASTER_SUPERSTEP_WAIT_MSECS = "giraph.maxMasterSuperstepWaitMsecs";
  /**
   * Default maximum milliseconds to wait before giving up trying to get
   * the minimum number of workers before a superstep (10 minutes).
   */
  int MAX_MASTER_SUPERSTEP_WAIT_MSECS_DEFAULT = 10 * 60 * 1000;

  /** Milliseconds for a request to complete (or else resend) */
  String MAX_REQUEST_MILLISECONDS = "giraph.maxRequestMilliseconds";
  /** Maximum number of milliseconds for a request to complete (10 minutes) */
  int MAX_REQUEST_MILLISECONDS_DEFAULT = 10 * 60 * 1000;

  /** Netty max connection failures */
  String NETTY_MAX_CONNECTION_FAILURES = "giraph.nettyMaxConnectionFailures";
  /** Default Netty max connection failures */
  int NETTY_MAX_CONNECTION_FAILURES_DEFAULT = 1000;

  /** Initial port to start using for the IPC communication */
  String IPC_INITIAL_PORT = "giraph.ipcInitialPort";
  /** Default port to start using for the IPC communication */
  int IPC_INITIAL_PORT_DEFAULT = 30000;

  /** Maximum bind attempts for different IPC ports */
  String MAX_IPC_PORT_BIND_ATTEMPTS = "giraph.maxIpcPortBindAttempts";
  /** Default maximum bind attempts for different IPC ports */
  int MAX_IPC_PORT_BIND_ATTEMPTS_DEFAULT = 20;
  /**
   * Fail first IPC port binding attempt, simulate binding failure
   * on real grid testing
   */
  String FAIL_FIRST_IPC_PORT_BIND_ATTEMPT =
      "giraph.failFirstIpcPortBindAttempt";
  /** Default fail first IPC port binding attempt flag */
  boolean FAIL_FIRST_IPC_PORT_BIND_ATTEMPT_DEFAULT = false;

  /** Client send buffer size */
  String CLIENT_SEND_BUFFER_SIZE = "giraph.clientSendBufferSize";
  /** Default client send buffer size of 0.5 MB */
  int DEFAULT_CLIENT_SEND_BUFFER_SIZE = 512 * 1024;

  /** Client receive buffer size */
  String CLIENT_RECEIVE_BUFFER_SIZE = "giraph.clientReceiveBufferSize";
  /** Default client receive buffer size of 32 k */
  int DEFAULT_CLIENT_RECEIVE_BUFFER_SIZE = 32 * 1024;

  /** Server send buffer size */
  String SERVER_SEND_BUFFER_SIZE = "giraph.serverSendBufferSize";
  /** Default server send buffer size of 32 k */
  int DEFAULT_SERVER_SEND_BUFFER_SIZE = 32 * 1024;

  /** Server receive buffer size */
  String SERVER_RECEIVE_BUFFER_SIZE = "giraph.serverReceiveBufferSize";
  /** Default server receive buffer size of 0.5 MB */
  int DEFAULT_SERVER_RECEIVE_BUFFER_SIZE = 512 * 1024;

  /** Maximum size of messages (in bytes) per peer before flush */
  String MAX_MSG_REQUEST_SIZE = "giraph.msgRequestSize";
  /** Default maximum size of messages per peer before flush of 0.5MB */
  int MAX_MSG_REQUEST_SIZE_DEFAULT = 512 * 1024;

  /** Maximum number of messages per peer before flush */
  String MSG_SIZE = "giraph.msgSize";
  /** Default maximum number of messages per peer before flush */
  int MSG_SIZE_DEFAULT = 2000;

  /** Maximum number of mutations per partition before flush */
  String MAX_MUTATIONS_PER_REQUEST = "giraph.maxMutationsPerRequest";
  /** Default maximum number of mutations per partition before flush */
  int MAX_MUTATIONS_PER_REQUEST_DEFAULT = 100;

  /**
   * Use message size encoding (typically better for complex objects,
   * not meant for primitive wrapped messages)
   */
  String USE_MESSAGE_SIZE_ENCODING = "giraph.useMessageSizeEncoding";
  /**
   * By default, do not use message size encoding as it is experimental.
   */
  boolean USE_MESSAGE_SIZE_ENCODING_DEFAULT = false;

  /** Number of channels used per server */
  String CHANNELS_PER_SERVER = "giraph.channelsPerServer";
  /** Default number of channels used per server of 1 */
  int DEFAULT_CHANNELS_PER_SERVER = 1;

  /** Number of flush threads per peer */
  String MSG_NUM_FLUSH_THREADS = "giraph.msgNumFlushThreads";

  /** Number of threads for vertex computation */
  String NUM_COMPUTE_THREADS = "giraph.numComputeThreads";
  /** Default number of threads for vertex computation */
  int NUM_COMPUTE_THREADS_DEFAULT = 1;

  /** Number of threads for input splits loading */
  String NUM_INPUT_SPLITS_THREADS = "giraph.numInputSplitsThreads";
  /** Default number of threads for input splits loading */
  int NUM_INPUT_SPLITS_THREADS_DEFAULT = 1;

  /** Minimum stragglers of the superstep before printing them out */
  String PARTITION_LONG_TAIL_MIN_PRINT = "giraph.partitionLongTailMinPrint";
  /** Only print stragglers with one as a default */
  int PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT = 1;

  /** Use superstep counters? (boolean) */
  String USE_SUPERSTEP_COUNTERS = "giraph.useSuperstepCounters";
  /** Default is to use the superstep counters */
  boolean USE_SUPERSTEP_COUNTERS_DEFAULT = true;

  /**
   * Set the multiplicative factor of how many partitions to create from
   * a single InputSplit based on the number of total InputSplits.  For
   * example, if there are 10 total InputSplits and this is set to 0.5, then
   * you will get 0.5 * 10 = 5 partitions for every InputSplit (given that the
   * minimum size is met).
   */
  String TOTAL_INPUT_SPLIT_MULTIPLIER = "giraph.totalInputSplitMultiplier";

  /**
   * Input split sample percent - Used only for sampling and testing, rather
   * than an actual job.  The idea is that to test, you might only want a
   * fraction of the actual input splits from your VertexInputFormat to
   * load (values should be [0, 100]).
   */
  String INPUT_SPLIT_SAMPLE_PERCENT = "giraph.inputSplitSamplePercent";
  /** Default is to use all the input splits */
  float INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT = 100f;

  /**
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of vertices loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  String INPUT_SPLIT_MAX_VERTICES = "giraph.InputSplitMaxVertices";
  /**
   * Default is that all the vertices are to be loaded from the input split
   */
  long INPUT_SPLIT_MAX_VERTICES_DEFAULT = -1;

  /**
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of edges loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  String INPUT_SPLIT_MAX_EDGES = "giraph.InputSplitMaxEdges";
  /**
   * Default is that all the edges are to be loaded from the input split
   */
  long INPUT_SPLIT_MAX_EDGES_DEFAULT = -1;

  /**
   * To minimize network usage when reading input splits,
   * each worker can prioritize splits that reside on its host.
   * This, however, comes at the cost of increased load on ZooKeeper.
   * Hence, users with a lot of splits and input threads (or with
   * configurations that can't exploit locality) may want to disable it.
   */
  String USE_INPUT_SPLIT_LOCALITY = "giraph.useInputSplitLocality";

  /**
   * Default is to prioritize local input splits.
   */
  boolean USE_INPUT_SPLIT_LOCALITY_DEFAULT = true;

  /** Java opts passed to ZooKeeper startup */
  String ZOOKEEPER_JAVA_OPTS = "giraph.zkJavaOpts";
  /** Default java opts passed to ZooKeeper startup */
  String ZOOKEEPER_JAVA_OPTS_DEFAULT =
      "-Xmx512m -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC " +
          "-XX:CMSInitiatingOccupancyFraction=70 -XX:MaxGCPauseMillis=100";

  /**
   *  How often to checkpoint (i.e. 0, means no checkpoint,
   *  1 means every superstep, 2 is every two supersteps, etc.).
   */
  String CHECKPOINT_FREQUENCY = "giraph.checkpointFrequency";

  /** Default checkpointing frequency of none. */
  int CHECKPOINT_FREQUENCY_DEFAULT = 0;

  /**
   * Delete checkpoints after a successful job run?
   */
  String CLEANUP_CHECKPOINTS_AFTER_SUCCESS =
      "giraph.cleanupCheckpointsAfterSuccess";
  /** Default is to clean up the checkponts after a successful job */
  boolean CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT = true;

  /**
   * An application can be restarted manually by selecting a superstep.  The
   * corresponding checkpoint must exist for this to work.  The user should
   * set a long value.  Default is start from scratch.
   */
  String RESTART_SUPERSTEP = "giraph.restartSuperstep";

  /**
   * Base ZNode for Giraph's state in the ZooKeeper cluster.  Must be a root
   * znode on the cluster beginning with "/"
   */
  String BASE_ZNODE_KEY = "giraph.zkBaseZNode";

  /**
   * If ZOOKEEPER_LIST is not set, then use this directory to manage
   * ZooKeeper
   */
  String ZOOKEEPER_MANAGER_DIRECTORY = "giraph.zkManagerDirectory";
  /**
   * Default ZooKeeper manager directory (where determining the servers in
   * HDFS files will go).  directory path will also have job number
   * for uniqueness.
   */
  String ZOOKEEPER_MANAGER_DIR_DEFAULT = "_bsp/_defaultZkManagerDir";

  /** Number of ZooKeeper client connection attempts before giving up. */
  String ZOOKEEPER_CONNECTION_ATTEMPTS = "giraph.zkConnectionAttempts";
  /** Default of 10 ZooKeeper client connection attempts before giving up. */
  int ZOOKEEPER_CONNECTION_ATTEMPTS_DEFAULT = 10;

  /** This directory has/stores the available checkpoint files in HDFS. */
  String CHECKPOINT_DIRECTORY = "giraph.checkpointDirectory";
  /**
   * Default checkpoint directory (where checkpoing files go in HDFS).  Final
   * directory path will also have the job number for uniqueness
   */
  String CHECKPOINT_DIRECTORY_DEFAULT = "_bsp/_checkpoints/";

  /** Directory in the local file system for out-of-core messages. */
  String MESSAGES_DIRECTORY = "giraph.messagesDirectory";
  /**
   * Default messages directory. directory path will also have the
   * job number for uniqueness
   */
  String MESSAGES_DIRECTORY_DEFAULT = "_bsp/_messages/";

  /** Whether or not to use out-of-core messages */
  String USE_OUT_OF_CORE_MESSAGES = "giraph.useOutOfCoreMessages";
  /** Default choice about using out-of-core messaging */
  boolean USE_OUT_OF_CORE_MESSAGES_DEFAULT = false;
  /**
   * If using out-of-core messaging, it tells how much messages do we keep
   * in memory.
   */
  String MAX_MESSAGES_IN_MEMORY = "giraph.maxMessagesInMemory";
  /** Default maximum number of messages in memory. */
  int MAX_MESSAGES_IN_MEMORY_DEFAULT = 1000000;
  /** Size of buffer when reading and writing messages out-of-core. */
  String MESSAGES_BUFFER_SIZE = "giraph.messagesBufferSize";
  /** Default size of buffer when reading and writing messages out-of-core. */
  int MESSAGES_BUFFER_SIZE_DEFAULT = 8192;

  /** Directory in the local filesystem for out-of-core partitions. */
  String PARTITIONS_DIRECTORY = "giraph.partitionsDirectory";
  /** Default directory for out-of-core partitions. */
  String PARTITIONS_DIRECTORY_DEFAULT = "_bsp/_partitions";

  /** Enable out-of-core graph. */
  String USE_OUT_OF_CORE_GRAPH = "giraph.useOutOfCoreGraph";
  /** Default is not to use out-of-core graph. */
  boolean USE_OUT_OF_CORE_GRAPH_DEFAULT = false;

  /** Maximum number of partitions to hold in memory for each worker. */
  String MAX_PARTITIONS_IN_MEMORY = "giraph.maxPartitionsInMemory";
  /** Default maximum number of in-memory partitions. */
  int MAX_PARTITIONS_IN_MEMORY_DEFAULT = 10;

  /** Keep the zookeeper output for debugging? Default is to remove it. */
  String KEEP_ZOOKEEPER_DATA = "giraph.keepZooKeeperData";
  /** Default is to remove ZooKeeper data. */
  Boolean KEEP_ZOOKEEPER_DATA_DEFAULT = false;

  /** Default ZooKeeper tick time. */
  int DEFAULT_ZOOKEEPER_TICK_TIME = 6000;
  /** Default ZooKeeper init limit (in ticks). */
  int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;
  /** Default ZooKeeper sync limit (in ticks). */
  int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;
  /** Default ZooKeeper snap count. */
  int DEFAULT_ZOOKEEPER_SNAP_COUNT = 50000;
  /** Default ZooKeeper maximum client connections. */
  int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 10000;
  /** ZooKeeper minimum session timeout */
  String ZOOKEEPER_MIN_SESSION_TIMEOUT = "giraph.zKMinSessionTimeout";
  /** Default ZooKeeper minimum session timeout of 10 minutes (in msecs). */
  int DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT = 600 * 1000;
  /** ZooKeeper maximum session timeout */
  String ZOOKEEPER_MAX_SESSION_TIMEOUT = "giraph.zkMaxSessionTimeout";
  /** Default ZooKeeper maximum session timeout of 15 minutes (in msecs). */
  int DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT = 900 * 1000;
  /** ZooKeeper force sync */
  String ZOOKEEPER_FORCE_SYNC = "giraph.zKForceSync";
  /** Default ZooKeeper force sync is off (for performance) */
  String DEFAULT_ZOOKEEPER_FORCE_SYNC = "no";
  /** ZooKeeper skip ACLs */
  String ZOOKEEPER_SKIP_ACL = "giraph.ZkSkipAcl";
  /** Default ZooKeeper skip ACLs true (for performance) */
  String DEFAULT_ZOOKEEPER_SKIP_ACL = "yes";

  /**
   * Whether to use SASL with DIGEST and Hadoop Job Tokens to authenticate
   * and authorize Netty BSP Clients to Servers.
   */
  String AUTHENTICATE = "giraph.authenticate";
  /** Default is not to do authenticate and authorization with Netty. */
  boolean DEFAULT_AUTHENTICATE = false;

  /** Use unsafe serialization? */
  String USE_UNSAFE_SERIALIZATION = "giraph.useUnsafeSerialization";
  /**
   * Use unsafe serialization default is true (use it if you can,
   * its much faster)!
   */
  boolean USE_UNSAFE_SERIALIZATION_DEFAULT = true;

  /**
   * Maximum number of attempts a master/worker will retry before killing
   * the job.  This directly maps to the number of map task attempts in
   * Hadoop.
   */
  String MAX_TASK_ATTEMPTS = "mapred.map.max.attempts";
}
// CHECKSTYLE: resume InterfaceIsTypeCheck
