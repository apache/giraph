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

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.DefaultVertexValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.job.DefaultJobObserver;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.partition.DefaultPartitionContext;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.partition.SimplePartition;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Constants used all over Giraph for configuration.
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface GiraphConstants {
  /** 1KB in bytes */
  int ONE_KB = 1024;

  /** Vertex class - required */
  ClassConfOption<Vertex> VERTEX_CLASS =
      ClassConfOption.create("giraph.vertexClass", null, Vertex.class);
  /** Vertex value factory class - optional */
  ClassConfOption<VertexValueFactory> VERTEX_VALUE_FACTORY_CLASS =
      ClassConfOption.create("giraph.vertexValueFactoryClass",
          DefaultVertexValueFactory.class, VertexValueFactory.class);
  /** Vertex edges class - optional */
  ClassConfOption<OutEdges> VERTEX_EDGES_CLASS =
      ClassConfOption.create("giraph.outEdgesClass", ByteArrayEdges.class,
          OutEdges.class);
  /** Vertex edges class to be used during edge input only - optional */
  ClassConfOption<OutEdges> INPUT_VERTEX_EDGES_CLASS =
      ClassConfOption.create("giraph.inputOutEdgesClass",
          ByteArrayEdges.class, OutEdges.class);

  /** Class for Master - optional */
  ClassConfOption<MasterCompute> MASTER_COMPUTE_CLASS =
      ClassConfOption.create("giraph.masterComputeClass",
          DefaultMasterCompute.class, MasterCompute.class);
  /** Classes for Master Observer - optional */
  ClassConfOption<MasterObserver> MASTER_OBSERVER_CLASSES =
      ClassConfOption.create("giraph.master.observers",
          null, MasterObserver.class);
  /** Classes for Worker Observer - optional */
  ClassConfOption<WorkerObserver> WORKER_OBSERVER_CLASSES =
      ClassConfOption.create("giraph.worker.observers", null,
          WorkerObserver.class);
  /** Vertex combiner class - optional */
  ClassConfOption<Combiner> VERTEX_COMBINER_CLASS =
      ClassConfOption.create("giraph.combinerClass", null, Combiner.class);
  /** Vertex resolver class - optional */
  ClassConfOption<VertexResolver> VERTEX_RESOLVER_CLASS =
      ClassConfOption.create("giraph.vertexResolverClass",
          DefaultVertexResolver.class, VertexResolver.class);

  /**
   * Option of whether to create vertexes that were not existent before but
   * received messages
   */
  BooleanConfOption RESOLVER_CREATE_VERTEX_ON_MSGS =
      new BooleanConfOption("giraph.vertex.resolver.create.on.msgs", true);
  /** Graph partitioner factory class - optional */
  ClassConfOption<GraphPartitionerFactory> GRAPH_PARTITIONER_FACTORY_CLASS =
      ClassConfOption.create("giraph.graphPartitionerFactoryClass",
          HashPartitionerFactory.class, GraphPartitionerFactory.class);

  /** Observer class to watch over job status - optional */
  ClassConfOption<GiraphJobObserver> JOB_OBSERVER_CLASS =
      ClassConfOption.create("giraph.jobObserverClass",
          DefaultJobObserver.class, GiraphJobObserver.class);

  // At least one of the input format classes is required.
  /** VertexInputFormat class */
  ClassConfOption<VertexInputFormat> VERTEX_INPUT_FORMAT_CLASS =
      ClassConfOption.create("giraph.vertexInputFormatClass", null,
          VertexInputFormat.class);
  /** EdgeInputFormat class */
  ClassConfOption<EdgeInputFormat> EDGE_INPUT_FORMAT_CLASS =
      ClassConfOption.create("giraph.edgeInputFormatClass", null,
          EdgeInputFormat.class);

  /** VertexOutputFormat class */
  ClassConfOption<VertexOutputFormat> VERTEX_OUTPUT_FORMAT_CLASS =
      ClassConfOption.create("giraph.vertexOutputFormatClass", null,
          VertexOutputFormat.class);
  /**
   * If you use this option, instead of having saving vertices in the end of
   * application, saveVertex will be called right after each vertex.compute()
   * is called.
   * NOTE: This feature doesn't work well with checkpointing - if you restart
   * from a checkpoint you won't have any ouptut from previous supresteps.
   */
  BooleanConfOption DO_OUTPUT_DURING_COMPUTATION =
      new BooleanConfOption("giraph.doOutputDuringComputation", false);
  /**
   * Vertex output format thread-safe - if your VertexOutputFormat allows
   * several vertexWriters to be created and written to in parallel,
   * you should set this to true.
   */
  BooleanConfOption VERTEX_OUTPUT_FORMAT_THREAD_SAFE =
      new BooleanConfOption("giraph.vertexOutputFormatThreadSafe", false);
  /** Number of threads for writing output in the end of the application */
  IntConfOption NUM_OUTPUT_THREADS =
      new IntConfOption("giraph.numOutputThreads", 1);

  /** conf key for comma-separated list of jars to export to YARN workers */
  StrConfOption GIRAPH_YARN_LIBJARS =
    new StrConfOption("giraph.yarn.libjars", "");
  /** Name of the XML file that will export our Configuration to YARN workers */
  String GIRAPH_YARN_CONF_FILE = "giraph-conf.xml";
  /** Giraph default heap size for all tasks when running on YARN profile */
  int GIRAPH_YARN_TASK_HEAP_MB_DEFAULT = 1024;
  /** Name of Giraph property for user-configurable heap memory per worker */
  IntConfOption GIRAPH_YARN_TASK_HEAP_MB = new IntConfOption(
    "giraph.yarn.task.heap.mb", GIRAPH_YARN_TASK_HEAP_MB_DEFAULT);
  /** Default priority level in YARN for our task containers */
  int GIRAPH_YARN_PRIORITY = 10;
  /** Is this a pure YARN job (i.e. no MapReduce layer managing Giraph tasks) */
  BooleanConfOption IS_PURE_YARN_JOB =
    new BooleanConfOption("giraph.pure.yarn.job", false);

  /** Vertex index class */
  ClassConfOption<WritableComparable> VERTEX_ID_CLASS =
      ClassConfOption.create("giraph.vertexIdClass", null,
          WritableComparable.class);
  /** Vertex value class */
  ClassConfOption<Writable> VERTEX_VALUE_CLASS =
      ClassConfOption.create("giraph.vertexValueClass", null, Writable.class);
  /** Edge value class */
  ClassConfOption<Writable> EDGE_VALUE_CLASS =
      ClassConfOption.create("giraph.edgeValueClass", null, Writable.class);
  /** Message value class */
  ClassConfOption<Writable> MESSAGE_VALUE_CLASS =
      ClassConfOption.create("giraph.messageValueClass", null, Writable.class);
  /** Partition context class */
  ClassConfOption<PartitionContext> PARTITION_CONTEXT_CLASS =
      ClassConfOption.create("giraph.partitionContextClass",
          DefaultPartitionContext.class, PartitionContext.class);
  /** Worker context class */
  ClassConfOption<WorkerContext> WORKER_CONTEXT_CLASS =
      ClassConfOption.create("giraph.workerContextClass",
          DefaultWorkerContext.class, WorkerContext.class);
  /** AggregatorWriter class - optional */
  ClassConfOption<AggregatorWriter> AGGREGATOR_WRITER_CLASS =
      ClassConfOption.create("giraph.aggregatorWriterClass",
          TextAggregatorWriter.class, AggregatorWriter.class);

  /** Partition class - optional */
  ClassConfOption<Partition> PARTITION_CLASS =
      ClassConfOption.create("giraph.partitionClass", SimplePartition.class,
          Partition.class);

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
  BooleanConfOption SPLIT_MASTER_WORKER =
      new BooleanConfOption("giraph.SplitMasterWorker", true);

  /** Indicates whether this job is run in an internal unit test */
  BooleanConfOption LOCAL_TEST_MODE =
      new BooleanConfOption("giraph.localTestMode", false);

  /** Override the Hadoop log level and set the desired log level. */
  StrConfOption LOG_LEVEL = new StrConfOption("giraph.logLevel", "info");

  /** Use thread level debugging? */
  BooleanConfOption LOG_THREAD_LAYOUT =
      new BooleanConfOption("giraph.logThreadLayout", false);

  /** Configuration key to enable jmap printing */
  BooleanConfOption JMAP_ENABLE =
      new BooleanConfOption("giraph.jmap.histo.enable", false);

  /** Configuration key for msec to sleep between calls */
  IntConfOption JMAP_SLEEP_MILLIS =
      new IntConfOption("giraph.jmap.histo.msec", SECONDS.toMillis(30));

  /** Configuration key for how many lines to print */
  IntConfOption JMAP_PRINT_LINES =
      new IntConfOption("giraph.jmap.histo.print_lines", 30);

  /**
   * Minimum percent of the maximum number of workers that have responded
   * in order to continue progressing. (float)
   */
  FloatConfOption MIN_PERCENT_RESPONDED =
      new FloatConfOption("giraph.minPercentResponded", 100.0f);

  /** Enable the Metrics system **/
  BooleanConfOption METRICS_ENABLE =
      new BooleanConfOption("giraph.metrics.enable", false);

  /**
   *  ZooKeeper comma-separated list (if not set,
   *  will start up ZooKeeper locally)
   */
  String ZOOKEEPER_LIST = "giraph.zkList";

  /** ZooKeeper session millisecond timeout */
  IntConfOption ZOOKEEPER_SESSION_TIMEOUT =
      new IntConfOption("giraph.zkSessionMsecTimeout", MINUTES.toMillis(1));

  /** Polling interval to check for the ZooKeeper server data */
  IntConfOption ZOOKEEPER_SERVERLIST_POLL_MSECS =
      new IntConfOption("giraph.zkServerlistPollMsecs", SECONDS.toMillis(3));

  /** Number of nodes (not tasks) to run Zookeeper on */
  IntConfOption ZOOKEEPER_SERVER_COUNT =
      new IntConfOption("giraph.zkServerCount", 1);

  /** ZooKeeper port to use */
  IntConfOption ZOOKEEPER_SERVER_PORT =
      new IntConfOption("giraph.zkServerPort", 22181);

  /** Location of the ZooKeeper jar - Used internally, not meant for users */
  String ZOOKEEPER_JAR = "giraph.zkJar";

  /** Local ZooKeeper directory to use */
  String ZOOKEEPER_DIR = "giraph.zkDir";

  /** Max attempts for handling ZooKeeper connection loss */
  IntConfOption ZOOKEEPER_OPS_MAX_ATTEMPTS =
      new IntConfOption("giraph.zkOpsMaxAttempts", 3);

  /**
   * Msecs to wait before retrying a failed ZooKeeper op due to connection loss.
   */
  IntConfOption ZOOKEEPER_OPS_RETRY_WAIT_MSECS =
      new IntConfOption("giraph.zkOpsRetryWaitMsecs", SECONDS.toMillis(5));

  /** TCP backlog (defaults to number of workers) */
  IntConfOption TCP_BACKLOG = new IntConfOption("giraph.tcpBacklog", 1);

  /** How big to make the encoder buffer? */
  IntConfOption NETTY_REQUEST_ENCODER_BUFFER_SIZE =
      new IntConfOption("giraph.nettyRequestEncoderBufferSize", 32 * ONE_KB);

  /** Whether or not netty request encoder should use direct byte buffers */
  BooleanConfOption NETTY_REQUEST_ENCODER_USE_DIRECT_BUFFERS =
      new BooleanConfOption("giraph.nettyRequestEncoderUseDirectBuffers",
                            false);

  /** Netty client threads */
  IntConfOption NETTY_CLIENT_THREADS =
      new IntConfOption("giraph.nettyClientThreads", 4);

  /** Netty server threads */
  IntConfOption NETTY_SERVER_THREADS =
      new IntConfOption("giraph.nettyServerThreads", 16);

  /** Use the execution handler in netty on the client? */
  BooleanConfOption NETTY_CLIENT_USE_EXECUTION_HANDLER =
      new BooleanConfOption("giraph.nettyClientUseExecutionHandler", true);

  /** Netty client execution threads (execution handler) */
  IntConfOption NETTY_CLIENT_EXECUTION_THREADS =
      new IntConfOption("giraph.nettyClientExecutionThreads", 8);

  /** Where to place the netty client execution handle? */
  StrConfOption NETTY_CLIENT_EXECUTION_AFTER_HANDLER =
      new StrConfOption("giraph.nettyClientExecutionAfterHandler",
          "requestEncoder");

  /** Use the execution handler in netty on the server? */
  BooleanConfOption NETTY_SERVER_USE_EXECUTION_HANDLER =
      new BooleanConfOption("giraph.nettyServerUseExecutionHandler", true);

  /** Netty server execution threads (execution handler) */
  IntConfOption NETTY_SERVER_EXECUTION_THREADS =
      new IntConfOption("giraph.nettyServerExecutionThreads", 8);

  /** Where to place the netty server execution handle? */
  StrConfOption NETTY_SERVER_EXECUTION_AFTER_HANDLER =
      new StrConfOption("giraph.nettyServerExecutionAfterHandler",
          "requestFrameDecoder");

  /** Netty simulate a first request closed */
  BooleanConfOption NETTY_SIMULATE_FIRST_REQUEST_CLOSED =
      new BooleanConfOption("giraph.nettySimulateFirstRequestClosed", false);

  /** Netty simulate a first response failed */
  BooleanConfOption NETTY_SIMULATE_FIRST_RESPONSE_FAILED =
      new BooleanConfOption("giraph.nettySimulateFirstResponseFailed", false);

  /** Max resolve address attempts */
  IntConfOption MAX_RESOLVE_ADDRESS_ATTEMPTS =
      new IntConfOption("giraph.maxResolveAddressAttempts", 5);

  /** Msecs to wait between waiting for all requests to finish */
  IntConfOption WAITING_REQUEST_MSECS =
      new IntConfOption("giraph.waitingRequestMsecs", SECONDS.toMillis(15));

  /** Millseconds to wait for an event before continuing */
  IntConfOption EVENT_WAIT_MSECS =
      new IntConfOption("giraph.eventWaitMsecs", SECONDS.toMillis(30));

  /**
   * Maximum milliseconds to wait before giving up trying to get the minimum
   * number of workers before a superstep (int).
   */
  IntConfOption MAX_MASTER_SUPERSTEP_WAIT_MSECS =
      new IntConfOption("giraph.maxMasterSuperstepWaitMsecs",
          MINUTES.toMillis(10));

  /** Milliseconds for a request to complete (or else resend) */
  IntConfOption MAX_REQUEST_MILLISECONDS =
      new IntConfOption("giraph.maxRequestMilliseconds", MINUTES.toMillis(10));

  /** Netty max connection failures */
  IntConfOption NETTY_MAX_CONNECTION_FAILURES =
      new IntConfOption("giraph.nettyMaxConnectionFailures", 1000);

  /** Initial port to start using for the IPC communication */
  IntConfOption IPC_INITIAL_PORT =
      new IntConfOption("giraph.ipcInitialPort", 30000);

  /** Maximum bind attempts for different IPC ports */
  IntConfOption MAX_IPC_PORT_BIND_ATTEMPTS =
      new IntConfOption("giraph.maxIpcPortBindAttempts", 20);
  /**
   * Fail first IPC port binding attempt, simulate binding failure
   * on real grid testing
   */
  BooleanConfOption FAIL_FIRST_IPC_PORT_BIND_ATTEMPT =
      new BooleanConfOption("giraph.failFirstIpcPortBindAttempt", false);

  /** Client send buffer size */
  IntConfOption CLIENT_SEND_BUFFER_SIZE =
      new IntConfOption("giraph.clientSendBufferSize", 512 * ONE_KB);

  /** Client receive buffer size */
  IntConfOption CLIENT_RECEIVE_BUFFER_SIZE =
      new IntConfOption("giraph.clientReceiveBufferSize", 32 * ONE_KB);

  /** Server send buffer size */
  IntConfOption SERVER_SEND_BUFFER_SIZE =
      new IntConfOption("giraph.serverSendBufferSize", 32 * ONE_KB);

  /** Server receive buffer size */
  IntConfOption SERVER_RECEIVE_BUFFER_SIZE =
      new IntConfOption("giraph.serverReceiveBufferSize", 512 * ONE_KB);

  /** Maximum size of messages (in bytes) per peer before flush */
  IntConfOption MAX_MSG_REQUEST_SIZE =
      new IntConfOption("giraph.msgRequestSize", 512 * ONE_KB);

  /**
   * How much bigger than the average per partition size to make initial per
   * partition buffers.
   * If this value is A, message request size is M,
   * and a worker has P partitions, than its initial partition buffer size
   * will be (M / P) * (1 + A).
   */
  FloatConfOption ADDITIONAL_MSG_REQUEST_SIZE =
      new FloatConfOption("giraph.additionalMsgRequestSize", 0.2f);

  /** Maximum size of edges (in bytes) per peer before flush */
  IntConfOption MAX_EDGE_REQUEST_SIZE =
      new IntConfOption("giraph.edgeRequestSize", 512 * ONE_KB);

  /**
   * Additional size (expressed as a ratio) of each per-partition buffer on
   * top of the average size.
   */
  FloatConfOption ADDITIONAL_EDGE_REQUEST_SIZE =
      new FloatConfOption("giraph.additionalEdgeRequestSize", 0.2f);

  /** Maximum number of mutations per partition before flush */
  IntConfOption MAX_MUTATIONS_PER_REQUEST =
      new IntConfOption("giraph.maxMutationsPerRequest", 100);

  /**
   * Whether we should reuse the same Edge object when adding edges from
   * requests.
   * This works with edge storage implementations that don't keep references
   * to the input Edge objects (e.g., ByteArrayVertex).
   */
  BooleanConfOption REUSE_INCOMING_EDGE_OBJECTS =
      new BooleanConfOption("giraph.reuseIncomingEdgeObjects", false);

  /**
   * Use message size encoding (typically better for complex objects,
   * not meant for primitive wrapped messages)
   */
  BooleanConfOption USE_MESSAGE_SIZE_ENCODING =
      new BooleanConfOption("giraph.useMessageSizeEncoding", false);

  /** Number of channels used per server */
  IntConfOption CHANNELS_PER_SERVER =
      new IntConfOption("giraph.channelsPerServer", 1);

  /** Number of flush threads per peer */
  String MSG_NUM_FLUSH_THREADS = "giraph.msgNumFlushThreads";

  /** Number of threads for vertex computation */
  IntConfOption NUM_COMPUTE_THREADS =
      new IntConfOption("giraph.numComputeThreads", 1);

  /** Number of threads for input split loading */
  IntConfOption NUM_INPUT_THREADS =
      new IntConfOption("giraph.numInputThreads", 1);

  /** Minimum stragglers of the superstep before printing them out */
  IntConfOption PARTITION_LONG_TAIL_MIN_PRINT =
      new IntConfOption("giraph.partitionLongTailMinPrint", 1);

  /** Use superstep counters? (boolean) */
  BooleanConfOption USE_SUPERSTEP_COUNTERS =
      new BooleanConfOption("giraph.useSuperstepCounters", true);

  /**
   * Input split sample percent - Used only for sampling and testing, rather
   * than an actual job.  The idea is that to test, you might only want a
   * fraction of the actual input splits from your VertexInputFormat to
   * load (values should be [0, 100]).
   */
  FloatConfOption INPUT_SPLIT_SAMPLE_PERCENT =
      new FloatConfOption("giraph.inputSplitSamplePercent", 100f);

  /**
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of vertices loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  LongConfOption INPUT_SPLIT_MAX_VERTICES =
      new LongConfOption("giraph.InputSplitMaxVertices", -1);

  /**
   * To limit outlier vertex input splits from producing too many vertices or
   * to help with testing, the number of edges loaded from an input split
   * can be limited.  By default, everything is loaded.
   */
  LongConfOption INPUT_SPLIT_MAX_EDGES =
      new LongConfOption("giraph.InputSplitMaxEdges", -1);

  /**
   * To minimize network usage when reading input splits,
   * each worker can prioritize splits that reside on its host.
   * This, however, comes at the cost of increased load on ZooKeeper.
   * Hence, users with a lot of splits and input threads (or with
   * configurations that can't exploit locality) may want to disable it.
   */
  BooleanConfOption USE_INPUT_SPLIT_LOCALITY =
      new BooleanConfOption("giraph.useInputSplitLocality", true);

  /** Multiplier for the current workers squared */
  FloatConfOption PARTITION_COUNT_MULTIPLIER =
      new FloatConfOption("giraph.masterPartitionCountMultiplier", 1.0f);

  /** Overrides default partition count calculation if not -1 */
  IntConfOption USER_PARTITION_COUNT =
      new IntConfOption("giraph.userPartitionCount", -1);

  /** Vertex key space size for
   * {@link org.apache.giraph.partition.SimpleRangeWorkerPartitioner}
   */
  String PARTITION_VERTEX_KEY_SPACE_SIZE = "giraph.vertexKeySpaceSize";

  /** Java opts passed to ZooKeeper startup */
  StrConfOption ZOOKEEPER_JAVA_OPTS =
      new StrConfOption("giraph.zkJavaOpts",
          "-Xmx512m -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC " +
          "-XX:CMSInitiatingOccupancyFraction=70 -XX:MaxGCPauseMillis=100");

  /**
   *  How often to checkpoint (i.e. 0, means no checkpoint,
   *  1 means every superstep, 2 is every two supersteps, etc.).
   */
  IntConfOption CHECKPOINT_FREQUENCY =
      new IntConfOption("giraph.checkpointFrequency", 0);

  /**
   * Delete checkpoints after a successful job run?
   */
  BooleanConfOption CLEANUP_CHECKPOINTS_AFTER_SUCCESS =
      new BooleanConfOption("giraph.cleanupCheckpointsAfterSuccess", true);

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
  StrConfOption ZOOKEEPER_MANAGER_DIRECTORY =
      new StrConfOption("giraph.zkManagerDirectory",
          "_bsp/_defaultZkManagerDir");

  /** Number of ZooKeeper client connection attempts before giving up. */
  IntConfOption ZOOKEEPER_CONNECTION_ATTEMPTS =
      new IntConfOption("giraph.zkConnectionAttempts", 10);

  /** This directory has/stores the available checkpoint files in HDFS. */
  StrConfOption CHECKPOINT_DIRECTORY =
      new StrConfOption("giraph.checkpointDirectory", "_bsp/_checkpoints/");

  /**
   * Comma-separated list of directories in the local file system for
   * out-of-core messages.
   */
  StrConfOption MESSAGES_DIRECTORY =
      new StrConfOption("giraph.messagesDirectory", "_bsp/_messages/");

  /** Whether or not to use out-of-core messages */
  BooleanConfOption USE_OUT_OF_CORE_MESSAGES =
      new BooleanConfOption("giraph.useOutOfCoreMessages", false);
  /**
   * If using out-of-core messaging, it tells how much messages do we keep
   * in memory.
   */
  IntConfOption MAX_MESSAGES_IN_MEMORY =
      new IntConfOption("giraph.maxMessagesInMemory", 1000000);
  /** Size of buffer when reading and writing messages out-of-core. */
  IntConfOption MESSAGES_BUFFER_SIZE =
      new IntConfOption("giraph.messagesBufferSize", 8 * ONE_KB);

  /**
   * Comma-separated list of directories in the local filesystem for
   * out-of-core partitions.
   */
  StrConfOption PARTITIONS_DIRECTORY =
      new StrConfOption("giraph.partitionsDirectory", "_bsp/_partitions");

  /** Enable out-of-core graph. */
  BooleanConfOption USE_OUT_OF_CORE_GRAPH =
      new BooleanConfOption("giraph.useOutOfCoreGraph", false);

  /** Maximum number of partitions to hold in memory for each worker. */
  IntConfOption MAX_PARTITIONS_IN_MEMORY =
      new IntConfOption("giraph.maxPartitionsInMemory", 10);

  /** Keep the zookeeper output for debugging? Default is to remove it. */
  BooleanConfOption KEEP_ZOOKEEPER_DATA =
      new BooleanConfOption("giraph.keepZooKeeperData", false);

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
  IntConfOption ZOOKEEPER_MIN_SESSION_TIMEOUT =
      new IntConfOption("giraph.zKMinSessionTimeout", MINUTES.toMillis(10));
  /** ZooKeeper maximum session timeout */
  IntConfOption ZOOKEEPER_MAX_SESSION_TIMEOUT =
      new IntConfOption("giraph.zkMaxSessionTimeout", MINUTES.toMillis(15));
  /** ZooKeeper force sync */
  BooleanConfOption ZOOKEEPER_FORCE_SYNC =
      new BooleanConfOption("giraph.zKForceSync", false);
  /** ZooKeeper skip ACLs */
  BooleanConfOption ZOOKEEPER_SKIP_ACL =
      new BooleanConfOption("giraph.ZkSkipAcl", true);

  /**
   * Whether to use SASL with DIGEST and Hadoop Job Tokens to authenticate
   * and authorize Netty BSP Clients to Servers.
   */
  BooleanConfOption AUTHENTICATE =
      new BooleanConfOption("giraph.authenticate", false);

  /** Use unsafe serialization? */
  BooleanConfOption USE_UNSAFE_SERIALIZATION =
      new BooleanConfOption("giraph.useUnsafeSerialization", true);

  /**
   * Maximum number of attempts a master/worker will retry before killing
   * the job.  This directly maps to the number of map task attempts in
   * Hadoop.
   */
  IntConfOption MAX_TASK_ATTEMPTS =
      new IntConfOption("mapred.map.max.attempts", -1);

  /** Interface to use for hostname resolution */
  StrConfOption DNS_INTERFACE =
      new StrConfOption("giraph.dns.interface", "default");
  /** Server for hostname resolution */
  StrConfOption DNS_NAMESERVER =
      new StrConfOption("giraph.dns.nameserver", "default");

  /**
   * The application will halt after this many supersteps is completed.  For
   * instance, if it is set to 3, the application will run at most 0, 1,
   * and 2 supersteps and then go into the shutdown superstep.
   */
  IntConfOption MAX_NUMBER_OF_SUPERSTEPS =
      new IntConfOption("giraph.maxNumberOfSupersteps", 1);

  /**
   * The application will not mutate the graph topology (the edges). It is used
   * to optimise out-of-core graph, by not writing back edges every time.
   */
  BooleanConfOption STATIC_GRAPH =
      new BooleanConfOption("giraph.isStaticGraph", false);
}
// CHECKSTYLE: resume InterfaceIsTypeCheck
