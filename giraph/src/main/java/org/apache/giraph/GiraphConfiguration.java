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
import org.apache.giraph.graph.Combiner;
import org.apache.giraph.graph.EdgeInputFormat;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.GraphPartitionerFactory;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.master.MasterObserver;
import org.apache.hadoop.conf.Configuration;

/**
 * Adds user methods specific to Giraph.  This will be put into an
 * ImmutableClassesGiraphConfiguration that provides the configuration plus
 * the immutable classes.
 */
public class GiraphConfiguration extends Configuration {
  /** Vertex class - required */
  public static final String VERTEX_CLASS = "giraph.vertexClass";

  /** Class for Master - optional */
  public static final String MASTER_COMPUTE_CLASS = "giraph.masterComputeClass";
  /** Classes for Observer Master - optional */
  public static final String MASTER_OBSERVER_CLASSES =
      "giraph.master.observers";
  /** Vertex combiner class - optional */
  public static final String VERTEX_COMBINER_CLASS =
      "giraph.combinerClass";
  /** Vertex resolver class - optional */
  public static final String VERTEX_RESOLVER_CLASS =
      "giraph.vertexResolverClass";
  /**
   * Option of whether to create vertices that were not existent before but
   * received messages
   */
  public static final String RESOLVER_CREATE_VERTEX_ON_MSGS =
      "giraph.vertex.resolver.create.on.msgs";
  /** Graph partitioner factory class - optional */
  public static final String GRAPH_PARTITIONER_FACTORY_CLASS =
      "giraph.graphPartitionerFactoryClass";

  // At least one of the input format classes is required.
  /** VertexInputFormat class */
  public static final String VERTEX_INPUT_FORMAT_CLASS =
      "giraph.vertexInputFormatClass";
  /** EdgeInputFormat class */
  public static final String EDGE_INPUT_FORMAT_CLASS =
      "giraph.edgeInputFormatClass";

  /** VertexOutputFormat class */
  public static final String VERTEX_OUTPUT_FORMAT_CLASS =
      "giraph.vertexOutputFormatClass";

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

  /** Partition class - optional */
  public static final String PARTITION_CLASS = "giraph.partitionClass";

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

  /** Use thread level debugging? */
  public static final String LOG_THREAD_LAYOUT = "giraph.logThreadLayout";
  /** Default to not use thread-level debugging */
  public static final boolean LOG_THREAD_LAYOUT_DEFAULT = false;

  /**
   * Minimum percent of the maximum number of workers that have responded
   * in order to continue progressing. (float)
   */
  public static final String MIN_PERCENT_RESPONDED =
      "giraph.minPercentResponded";
  /** Default 100% response rate for workers */
  public static final float MIN_PERCENT_RESPONDED_DEFAULT = 100.0f;

  /** Enable the Metrics system **/
  public static final String METRICS_ENABLE = "giraph.metrics.enable";

  /** Whether to dump all metrics when the job finishes */
  public static final String METRICS_DUMP_AT_END = "giraph.metrics.dump.at.end";

  /** Whether to print superstep metrics */
  public static final String METRICS_SUPERSTEP_PRINT = "giraph.metrics.print";

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

  /** Max attempts for handling ZooKeeper connection loss */
  public static final String ZOOKEEPER_OPS_MAX_ATTEMPTS =
      "giraph.zkOpsMaxAttempts";
  /** Default of 3 attempts for handling ZooKeeper connection loss */
  public static final int ZOOKEEPER_OPS_MAX_ATTEMPTS_DEFAULT = 3;

  /**
   * Msecs to wait before retrying a failed ZooKeeper op due to connection
   * loss.
   */
  public static final String ZOOKEEPER_OPS_RETRY_WAIT_MSECS =
      "giraph.zkOpsRetryWaitMsecs";
  /**
   * Default to wait 5 seconds before retrying a failed ZooKeeper op due to
   * connection loss.
   */
  public static final int ZOOKEEPER_OPS_RETRY_WAIT_MSECS_DEFAULT = 5 * 1000;

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

  /** Milliseconds to wait between waiting for all requests to finish */
  public static final String WAITING_REQUEST_MSECS =
      "giraph.waitingRequestMsecs";
  /**
   * Default milliseconds to wait between waiting for all requests to finish
   */
  public static final int WAITING_REQUEST_MSECS_DEFAULT = 15000;

  /** Millseconds to wait for an event before continuing */
  public static final String EVENT_WAIT_MSECS = "giraph.eventWaitMsecs";
  /**
   * Default milliseconds to wait for an event before continuing (30 seconds)
   */
  public static final int EVENT_WAIT_MSECS_DEFAULT = 30 * 1000;

  /**
   * Maximum milliseconds to wait before giving up trying to get the minimum
   * number of workers before a superstep (int).
   */
  public static final String MAX_MASTER_SUPERSTEP_WAIT_MSECS =
      "giraph.maxMasterSuperstepWaitMsecs";
  /**
   * Default maximum milliseconds to wait before giving up trying to get
   * the minimum number of workers before a superstep (10 minutes).
   */
  public static final int MAX_MASTER_SUPERSTEP_WAIT_MSECS_DEFAULT =
      10 * 60 * 1000;

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

  /** Initial port to start using for the IPC communication */
  public static final String IPC_INITIAL_PORT = "giraph.ipcInitialPort";
  /** Default port to start using for the IPC communication */
  public static final int IPC_INITIAL_PORT_DEFAULT = 30000;

  /** Maximum bind attempts for different IPC ports */
  public static final String MAX_IPC_PORT_BIND_ATTEMPTS =
      "giraph.maxIpcPortBindAttempts";
  /** Default maximum bind attempts for different IPC ports */
  public static final int MAX_IPC_PORT_BIND_ATTEMPTS_DEFAULT = 20;
  /**
   * Fail first IPC port binding attempt, simulate binding failure
   * on real grid testing
   */
  public static final String FAIL_FIRST_IPC_PORT_BIND_ATTEMPT =
      "giraph.failFirstIpcPortBindAttempt";
  /** Default fail first IPC port binding attempt flag */
  public static final boolean FAIL_FIRST_IPC_PORT_BIND_ATTEMPT_DEFAULT = false;

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

  /**
   * Use message size encoding (typically better for complex objects,
   * not meant for primitive wrapped messages)
   */
  public static final String USE_MESSAGE_SIZE_ENCODING =
      "giraph.useMessageSizeEncoding";
  /**
   * By default, do not use message size encoding as it is experimental.
   */
  public static final boolean USE_MESSAGE_SIZE_ENCODING_DEFAULT = false;

  /** Number of channels used per server */
  public static final String CHANNELS_PER_SERVER =
      "giraph.channelsPerServer";
  /** Default number of channels used per server of 1 */
  public static final int DEFAULT_CHANNELS_PER_SERVER = 1;

  /** Number of flush threads per peer */
  public static final String MSG_NUM_FLUSH_THREADS =
      "giraph.msgNumFlushThreads";

  /** Number of threads for vertex computation */
  public static final String NUM_COMPUTE_THREADS = "giraph.numComputeThreads";
  /** Default number of threads for vertex computation */
  public static final int NUM_COMPUTE_THREADS_DEFAULT = 1;

  /** Number of threads for input splits loading */
  public static final String NUM_INPUT_SPLITS_THREADS =
      "giraph.numInputSplitsThreads";
  /** Default number of threads for input splits loading */
  public static final int NUM_INPUT_SPLITS_THREADS_DEFAULT = 1;

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
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of vertices loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  public static final String INPUT_SPLIT_MAX_VERTICES =
      "giraph.InputSplitMaxVertices";
  /**
   * Default is that all the vertices are to be loaded from the input split
   */
  public static final long INPUT_SPLIT_MAX_VERTICES_DEFAULT = -1;

  /**
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of edges loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  public static final String INPUT_SPLIT_MAX_EDGES =
      "giraph.InputSplitMaxEdges";
  /**
   * Default is that all the edges are to be loaded from the input split
   */
  public static final long INPUT_SPLIT_MAX_EDGES_DEFAULT = -1;

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
   * Whether to use SASL with DIGEST and Hadoop Job Tokens to authenticate
   * and authorize Netty BSP Clients to Servers.
   */
  public static final String AUTHENTICATE = "giraph.authenticate";
  /** Default is not to do authenticate and authorization with Netty. */
  public static final boolean DEFAULT_AUTHENTICATE = false;

  /** Use unsafe serialization? */
  public static final String USE_UNSAFE_SERIALIZATION =
      "giraph.useUnsafeSerialization";
  /**
   * Use unsafe serialization default is true (use it if you can,
   * its much faster)!
   */
  public static final boolean USE_UNSAFE_SERIALIZATION_DEFAULT = true;

  /**
   * Maximum number of attempts a master/worker will retry before killing
   * the job.  This directly maps to the number of map task attempts in
   * Hadoop.
   */
  public static final String MAX_TASK_ATTEMPTS = "mapred.map.max.attempts";

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
   * Set the edge input format class (required)
   *
   * @param edgeInputFormatClass Determines how graph is input
   */
  public final void setEdgeInputFormatClass(
      Class<? extends EdgeInputFormat> edgeInputFormatClass) {
    setClass(EDGE_INPUT_FORMAT_CLASS,
        edgeInputFormatClass,
        EdgeInputFormat.class);
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
   * Add a MasterObserver class (optional)
   *
   * @param masterObserverClass MasterObserver class to add.
   */
  public final void addMasterObserverClass(
      Class<? extends MasterObserver> masterObserverClass) {
    addToClasses(MASTER_OBSERVER_CLASSES, masterObserverClass,
        MasterObserver.class);
  }

  /**
   * Add a class to a property that is a list of classes. If the property does
   * not exist it will be created.
   *
   * @param name String name of property.
   * @param klass interface of the class being set.
   * @param xface Class to add to the list.
   */
  public final void addToClasses(String name, Class<?> klass, Class<?> xface) {
    if (!xface.isAssignableFrom(klass)) {
      throw new RuntimeException(klass + " does not implement " +
          xface.getName());
    }
    String value;
    String klasses = get(name);
    if (klasses == null) {
      value = klass.getName();
    } else {
      value = klasses + "," + klass.getName();
    }
    set(name, value);
  }

  /**
   * Set mapping from a key name to a list of classes.
   *
   * @param name String key name to use.
   * @param xface interface of the classes being set.
   * @param klasses Classes to set.
   */
  public final void setClasses(String name, Class<?> xface,
                               Class<?> ... klasses) {
    String[] klassNames = new String[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      Class<?> klass = klasses[i];
      if (!xface.isAssignableFrom(klass)) {
        throw new RuntimeException(klass + " does not implement " +
            xface.getName());
      }
      klassNames[i] = klasses[i].getName();
    }
    setStrings(name, klassNames);
  }

  /**
   * Get classes from a property that all implement a given interface.
   *
   * @param name String name of property to fetch.
   * @param xface interface classes must implement.
   * @param defaultValue If not found, return this
   * @param <T> Generic type of interface class
   * @return array of Classes implementing interface specified.
   */
  public final <T> Class<? extends T>[] getClassesOfType(String name,
      Class<T> xface, Class<? extends T> ... defaultValue) {
    Class<?>[] klasses = getClasses(name, defaultValue);
    for (Class<?> klass : klasses) {
      if (!xface.isAssignableFrom(klass)) {
        throw new RuntimeException(klass + " is not assignable from " +
            xface.getName());
      }
    }
    return (Class<? extends T>[]) klasses;
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
      Class<? extends Combiner> vertexCombinerClass) {
    setClass(VERTEX_COMBINER_CLASS,
        vertexCombinerClass,
        Combiner.class);
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
    setClass(VERTEX_RESOLVER_CLASS, vertexResolverClass, VertexResolver.class);
  }

  /**
   * Whether to create a vertex that doesn't exist when it receives messages.
   * This only affects DefaultVertexResolver.
   *
   * @return true if we should create non existent vertices that get messages.
   */
  public final boolean getResolverCreateVertexOnMessages() {
    return getBoolean(RESOLVER_CREATE_VERTEX_ON_MSGS, true);
  }

  /**
   * Set whether to create non existent vertices when they receive messages.
   *
   * @param v true if we should create vertices when they get messages.
   */
  public final void setResolverCreateVertexOnMessages(boolean v) {
    setBoolean(RESOLVER_CREATE_VERTEX_ON_MSGS, v);
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
   * Set the partition class (optional)
   *
   * @param partitionClass Determines the why partitions are stored
   */
  public final void setPartitionClass(
      Class<? extends Partition> partitionClass) {
    setClass(PARTITION_CLASS,
        partitionClass,
        Partition.class);
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
   * Get array of MasterObserver classes set in the configuration.
   *
   * @return array of MasterObserver classes.
   */
  public Class<? extends MasterObserver>[] getMasterObserverClasses() {
    return getClassesOfType(MASTER_OBSERVER_CLASSES, MasterObserver.class);
  }

  /**
   * Whether to track, print, and aggregate metrics.
   *
   * @return true if metrics are enabled, false otherwise (default)
   */
  public boolean metricsEnabled() {
    return getBoolean(METRICS_ENABLE, false);
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

  /**
   * Use the log thread layout option?
   *
   * @return True if use the log thread layout option, false otherwise
   */
  public boolean useLogThreadLayout() {
    return getBoolean(LOG_THREAD_LAYOUT, LOG_THREAD_LAYOUT_DEFAULT);
  }

  public boolean getLocalTestMode() {
    return getBoolean(LOCAL_TEST_MODE, LOCAL_TEST_MODE_DEFAULT);
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

  public int getZookeeperOpsMaxAttempts() {
    return getInt(ZOOKEEPER_OPS_MAX_ATTEMPTS,
        ZOOKEEPER_OPS_MAX_ATTEMPTS_DEFAULT);
  }

  public int getZookeeperOpsRetryWaitMsecs() {
    return getInt(ZOOKEEPER_OPS_RETRY_WAIT_MSECS,
        ZOOKEEPER_OPS_RETRY_WAIT_MSECS_DEFAULT);
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

  /**
   * Use authentication? (if supported)
   *
   * @return True if should authenticate, false otherwise
   */
  public boolean authenticate() {
    return getBoolean(AUTHENTICATE, DEFAULT_AUTHENTICATE);
  }

  /**
   * Set the number of compute threads
   *
   * @param numComputeThreads Number of compute threads to use
   */
  public void setNumComputeThreads(int numComputeThreads) {
    setInt(NUM_COMPUTE_THREADS, numComputeThreads);
  }

  public int getNumComputeThreads() {
    return getInt(NUM_COMPUTE_THREADS, NUM_COMPUTE_THREADS_DEFAULT);
  }

  /**
   * Set the number of input split threads
   *
   * @param numInputSplitsThreads Number of input split threads to use
   */
  public void setNumInputSplitsThreads(int numInputSplitsThreads) {
    setInt(NUM_INPUT_SPLITS_THREADS, numInputSplitsThreads);
  }

  public int getNumInputSplitsThreads() {
    return getInt(NUM_INPUT_SPLITS_THREADS, NUM_INPUT_SPLITS_THREADS_DEFAULT);
  }

  public long getInputSplitMaxVertices() {
    return getLong(INPUT_SPLIT_MAX_VERTICES, INPUT_SPLIT_MAX_VERTICES_DEFAULT);
  }

  public long getInputSplitMaxEdges() {
    return getLong(INPUT_SPLIT_MAX_EDGES, INPUT_SPLIT_MAX_EDGES_DEFAULT);
  }

  /**
   * Set whether to use unsafe serialization
   *
   * @param useUnsafeSerialization If true, use unsafe serialization
   */
  public void useUnsafeSerialization(boolean useUnsafeSerialization) {
    setBoolean(USE_UNSAFE_SERIALIZATION, useUnsafeSerialization);
  }

  /**
   * Use message size encoding?  This feature may help with complex message
   * objects.
   *
   * @return Whether to use message size encoding
   */
  public boolean useMessageSizeEncoding() {
    return getBoolean(
        USE_MESSAGE_SIZE_ENCODING, USE_MESSAGE_SIZE_ENCODING_DEFAULT);
  }

  /**
   * Set the checkpoint frequeuncy of how many supersteps to wait before
   * checkpointing
   *
   * @param checkpointFrequency How often to checkpoint (0 means never)
   */
  public void setCheckpointFrequency(int checkpointFrequency) {
    setInt(CHECKPOINT_FREQUENCY, checkpointFrequency);
  }

  /**
   * Get the checkpoint frequeuncy of how many supersteps to wait
   * before checkpointing
   *
   * @return Checkpoint frequency (0 means never)
   */
  public int getCheckpointFrequency() {
    return getInt(CHECKPOINT_FREQUENCY, CHECKPOINT_FREQUENCY_DEFAULT);
  }

  /**
   * Set the max task attempts
   *
   * @param maxTaskAttempts Max task attempts to use
   */
  public void setMaxTaskAttempts(int maxTaskAttempts) {
    setInt(MAX_TASK_ATTEMPTS, maxTaskAttempts);
  }

  /**
   * Get the max task attempts
   *
   * @return Max task attempts or -1, if not set
   */
  public int getMaxTaskAttempts() {
    return getInt(MAX_TASK_ATTEMPTS, -1);
  }

  /**
   * Get the number of milliseconds to wait for an event before continuing on
   *
   * @return Number of milliseconds to wait for an event before continuing on
   */
  public int getEventWaitMsecs() {
    return getInt(EVENT_WAIT_MSECS, EVENT_WAIT_MSECS_DEFAULT);
  }

  /**
   * Set the number of milliseconds to wait for an event before continuing on
   *
   * @param eventWaitMsecs Number of milliseconds to wait for an event before
   *                       continuing on
   */
  public void setEventWaitMsecs(int eventWaitMsecs) {
    setInt(EVENT_WAIT_MSECS, eventWaitMsecs);
  }

  /**
   * Get the maximum milliseconds to wait before giving up trying to get the
   * minimum number of workers before a superstep.
   *
   * @return Maximum milliseconds to wait before giving up trying to get the
   *         minimum number of workers before a superstep
   */
  public int getMaxMasterSuperstepWaitMsecs() {
    return getInt(MAX_MASTER_SUPERSTEP_WAIT_MSECS,
        MAX_MASTER_SUPERSTEP_WAIT_MSECS_DEFAULT);
  }

  /**
   * Set the maximum milliseconds to wait before giving up trying to get the
   * minimum number of workers before a superstep.
   *
   * @param maxMasterSuperstepWaitMsecs Maximum milliseconds to wait before
   *                                    giving up trying to get the minimum
   *                                    number of workers before a superstep
   */
  public void setMaxMasterSuperstepWaitMsecs(int maxMasterSuperstepWaitMsecs) {
    setInt(MAX_MASTER_SUPERSTEP_WAIT_MSECS, maxMasterSuperstepWaitMsecs);
  }
}
