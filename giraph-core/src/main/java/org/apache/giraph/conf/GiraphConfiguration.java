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
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReuseObjectsOutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.partition.ReusesObjectsPartition;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;

import java.net.UnknownHostException;

/**
 * Adds user methods specific to Giraph.  This will be put into an
 * ImmutableClassesGiraphConfiguration that provides the configuration plus
 * the immutable classes.
 */
public class GiraphConfiguration extends Configuration
    implements GiraphConstants {
  /**
   * Constructor that creates the configuration
   */
  public GiraphConfiguration() {
    configureHadoopSecurity();
  }

  /**
   * Constructor.
   *
   * @param conf Configuration
   */
  public GiraphConfiguration(Configuration conf) {
    super(conf);
    configureHadoopSecurity();
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.graph.Vertex}
   *
   * @return User's vertex class
   */
  public Class<? extends Vertex> getVertexClass() {
    return VERTEX_CLASS.get(this);
  }

  /**
   * Set the vertex class (required)
   *
   * @param vertexClass Runs vertex computation
   */
  public final void setVertexClass(
      Class<? extends Vertex> vertexClass) {
    VERTEX_CLASS.set(this, vertexClass);
  }

  /**
   * Set the vertex value factory class
   *
   * @param vertexValueFactoryClass Creates default vertex values
   */
  public final void setVertexValueFactoryClass(
      Class<? extends VertexValueFactory> vertexValueFactoryClass) {
    VERTEX_VALUE_FACTORY_CLASS.set(this, vertexValueFactoryClass);
  }

  /**
   * Get the vertex edges class
   *
   * @return vertex edges class
   */
  public Class<? extends OutEdges> getOutEdgesClass() {
    return VERTEX_EDGES_CLASS.get(this);
  }

  /**
   * Set the vertex edges class
   *
   * @param outEdgesClass Determines the way edges are stored
   */
  public final void setOutEdgesClass(
      Class<? extends OutEdges> outEdgesClass) {
    VERTEX_EDGES_CLASS.set(this, outEdgesClass);
  }

  /**
   * Set the vertex edges class used during edge-based input (if different
   * from the one used during computation)
   *
   * @param inputOutEdgesClass Determines the way edges are stored
   */
  public final void setInputOutEdgesClass(
      Class<? extends OutEdges> inputOutEdgesClass) {
    INPUT_VERTEX_EDGES_CLASS.set(this, inputOutEdgesClass);
  }

  /**
   * True if the {@link org.apache.giraph.edge.OutEdges} implementation
   * copies the passed edges to its own data structure,
   * i.e. it doesn't keep references to Edge objects, target vertex ids or edge
   * values passed to add() or initialize().
   * This makes it possible to reuse edge objects passed to the data
   * structure, to minimize object instantiation (see for example
   * EdgeStore#addPartitionEdges()).
   *
   * @return True iff we can reuse the edge objects
   */
  public boolean reuseEdgeObjects() {
    return ReuseObjectsOutEdges.class.isAssignableFrom(
        getOutEdgesClass());
  }

  /**
   * True if the {@link Partition} implementation copies the passed vertices
   * to its own data structure, i.e. it doesn't keep references to Vertex
   * objects passed to it.
   * This makes it possible to reuse vertex objects passed to the data
   * structure, to minimize object instantiation.
   *
   * @return True iff we can reuse the vertex objects
   */
  public boolean reuseVertexObjects() {
    return ReusesObjectsPartition.class.isAssignableFrom(getPartitionClass());
  }

  /**
   * Get Partition class used
   * @return Partition class
   */
  public Class<? extends Partition> getPartitionClass() {
    return PARTITION_CLASS.get(this);
  }

  /**
   * Does the job have a {@link VertexInputFormat}?
   *
   * @return True iff a {@link VertexInputFormat} has been specified.
   */
  public boolean hasVertexInputFormat() {
    return VERTEX_INPUT_FORMAT_CLASS.get(this) != null;
  }

  /**
   * Set the vertex input format class (required)
   *
   * @param vertexInputFormatClass Determines how graph is input
   */
  public final void setVertexInputFormatClass(
      Class<? extends VertexInputFormat> vertexInputFormatClass) {
    VERTEX_INPUT_FORMAT_CLASS.set(this, vertexInputFormatClass);
  }

  /**
   * Does the job have a {@link EdgeInputFormat}?
   *
   * @return True iff a {@link EdgeInputFormat} has been specified.
   */
  public boolean hasEdgeInputFormat() {
    return EDGE_INPUT_FORMAT_CLASS.get(this) != null;
  }

  /**
   * Set the edge input format class (required)
   *
   * @param edgeInputFormatClass Determines how graph is input
   */
  public final void setEdgeInputFormatClass(
      Class<? extends EdgeInputFormat> edgeInputFormatClass) {
    EDGE_INPUT_FORMAT_CLASS.set(this, edgeInputFormatClass);
  }

  /**
   * Set the master class (optional)
   *
   * @param masterComputeClass Runs master computation
   */
  public final void setMasterComputeClass(
      Class<? extends MasterCompute> masterComputeClass) {
    MASTER_COMPUTE_CLASS.set(this, masterComputeClass);
  }

  /**
   * Add a MasterObserver class (optional)
   *
   * @param masterObserverClass MasterObserver class to add.
   */
  public final void addMasterObserverClass(
      Class<? extends MasterObserver> masterObserverClass) {
    MASTER_OBSERVER_CLASSES.add(this, masterObserverClass);
  }

  /**
   * Add a WorkerObserver class (optional)
   *
   * @param workerObserverClass WorkerObserver class to add.
   */
  public final void addWorkerObserverClass(
      Class<? extends WorkerObserver> workerObserverClass) {
    WORKER_OBSERVER_CLASSES.add(this, workerObserverClass);
  }

  /**
   * Get job observer class
   *
   * @return GiraphJobObserver class set.
   */
  public Class<? extends GiraphJobObserver> getJobObserverClass() {
    return JOB_OBSERVER_CLASS.get(this);
  }

  /**
   * Set job observer class
   *
   * @param klass GiraphJobObserver class to set.
   */
  public void setJobObserverClass(Class<? extends GiraphJobObserver> klass) {
    JOB_OBSERVER_CLASS.set(this, klass);
  }

  /**
   * Check whether to enable jmap dumping thread.
   *
   * @return true if jmap dumper is enabled.
   */
  public boolean isJMapHistogramDumpEnabled() {
    return JMAP_ENABLE.get(this);
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
   * Does the job have a {@link VertexOutputFormat}?
   *
   * @return True iff a {@link VertexOutputFormat} has been specified.
   */
  public boolean hasVertexOutputFormat() {
    return VERTEX_OUTPUT_FORMAT_CLASS.get(this) != null;
  }

  /**
   * Set the vertex output format class (optional)
   *
   * @param vertexOutputFormatClass Determines how graph is output
   */
  public final void setVertexOutputFormatClass(
      Class<? extends VertexOutputFormat> vertexOutputFormatClass) {
    VERTEX_OUTPUT_FORMAT_CLASS.set(this, vertexOutputFormatClass);
  }

  /**
   * Check if output should be done during computation
   *
   * @return True iff output should be done during computation
   */
  public final boolean doOutputDuringComputation() {
    return DO_OUTPUT_DURING_COMPUTATION.get(this);
  }

  /**
   * Set whether or not we should do output during computation
   *
   * @param doOutputDuringComputation True iff we want output to happen
   *                                  during computation
   */
  public final void setDoOutputDuringComputation(
      boolean doOutputDuringComputation) {
    DO_OUTPUT_DURING_COMPUTATION.set(this, doOutputDuringComputation);
  }

  /**
   * Check if VertexOutputFormat is thread-safe
   *
   * @return True iff VertexOutputFormat is thread-safe
   */
  public final boolean vertexOutputFormatThreadSafe() {
    return VERTEX_OUTPUT_FORMAT_THREAD_SAFE.get(this);
  }

  /**
   * Set whether or not selected VertexOutputFormat is thread-safe
   *
   * @param vertexOutputFormatThreadSafe True iff selected VertexOutputFormat
   *                                     is thread-safe
   */
  public final void setVertexOutputFormatThreadSafe(
      boolean vertexOutputFormatThreadSafe) {
    VERTEX_OUTPUT_FORMAT_THREAD_SAFE.set(this, vertexOutputFormatThreadSafe);
  }

  /**
   * Get the number of threads to use for writing output in the end of the
   * application. If output format is not thread safe, returns 1.
   *
   * @return Number of output threads
   */
  public final int getNumOutputThreads() {
    if (!vertexOutputFormatThreadSafe()) {
      return 1;
    } else {
      return NUM_OUTPUT_THREADS.get(this);
    }
  }

  /**
   * Set the number of threads to use for writing output in the end of the
   * application. Will be used only if {#vertexOutputFormatThreadSafe} is true.
   *
   * @param numOutputThreads Number of output threads
   */
  public void setNumOutputThreads(int numOutputThreads) {
    NUM_OUTPUT_THREADS.set(this, numOutputThreads);
  }

  /**
   * Get the vertex combiner class (optional)
   *
   * @return vertexCombinerClass Determines how vertex messages are combined
   */
  public Class<? extends Combiner> getCombinerClass() {
    return VERTEX_COMBINER_CLASS.get(this);
  }

  /**
   * Set the vertex combiner class (optional)
   *
   * @param vertexCombinerClass Determines how vertex messages are combined
   */
  public final void setCombinerClass(
      Class<? extends Combiner> vertexCombinerClass) {
    VERTEX_COMBINER_CLASS.set(this, vertexCombinerClass);
  }

  /**
   * Set the graph partitioner class (optional)
   *
   * @param graphPartitionerFactoryClass Determines how the graph is partitioned
   */
  public final void setGraphPartitionerFactoryClass(
      Class<? extends GraphPartitionerFactory> graphPartitionerFactoryClass) {
    GRAPH_PARTITIONER_FACTORY_CLASS.set(this, graphPartitionerFactoryClass);
  }

  /**
   * Set the vertex resolver class (optional)
   *
   * @param vertexResolverClass Determines how vertex mutations are resolved
   */
  public final void setVertexResolverClass(
      Class<? extends VertexResolver> vertexResolverClass) {
    VERTEX_RESOLVER_CLASS.set(this, vertexResolverClass);
  }

  /**
   * Whether to create a vertex that doesn't exist when it receives messages.
   * This only affects DefaultVertexResolver.
   *
   * @return true if we should create non existent vertices that get messages.
   */
  public final boolean getResolverCreateVertexOnMessages() {
    return RESOLVER_CREATE_VERTEX_ON_MSGS.get(this);
  }

  /**
   * Set whether to create non existent vertices when they receive messages.
   *
   * @param v true if we should create vertices when they get messages.
   */
  public final void setResolverCreateVertexOnMessages(boolean v) {
    RESOLVER_CREATE_VERTEX_ON_MSGS.set(this, v);
  }

  /**
   * Set the partition context class (optional)
   *
   * @param partitionContextClass Determines what code is executed for each
   *        partition before and after each superstep
   */
  public final void setPartitionContextClass(
      Class<? extends PartitionContext> partitionContextClass) {
    PARTITION_CONTEXT_CLASS.set(this, partitionContextClass);
  }

  /**
   * Set the worker context class (optional)
   *
   * @param workerContextClass Determines what code is executed on a each
   *        worker before and after each superstep and computation
   */
  public final void setWorkerContextClass(
      Class<? extends WorkerContext> workerContextClass) {
    WORKER_CONTEXT_CLASS.set(this, workerContextClass);
  }

  /**
   * Set the aggregator writer class (optional)
   *
   * @param aggregatorWriterClass Determines how the aggregators are
   *        written to file at the end of the job
   */
  public final void setAggregatorWriterClass(
      Class<? extends AggregatorWriter> aggregatorWriterClass) {
    AGGREGATOR_WRITER_CLASS.set(this, aggregatorWriterClass);
  }

  /**
   * Set the partition class (optional)
   *
   * @param partitionClass Determines the why partitions are stored
   */
  public final void setPartitionClass(
      Class<? extends Partition> partitionClass) {
    PARTITION_CLASS.set(this, partitionClass);
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
    MIN_PERCENT_RESPONDED.set(this, minPercentResponded);
  }

  public final int getMinWorkers() {
    return getInt(MIN_WORKERS, -1);
  }

  public final int getMaxWorkers() {
    return getInt(MAX_WORKERS, -1);
  }

  public final float getMinPercentResponded() {
    return MIN_PERCENT_RESPONDED.get(this);
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

  /**
   * Getter for SPLIT_MASTER_WORKER flag.
   * @return boolean flag value.
   */
  public final boolean getSplitMasterWorker() {
    return SPLIT_MASTER_WORKER.get(this);
  }

  /**
   * Get array of MasterObserver classes set in the configuration.
   *
   * @return array of MasterObserver classes.
   */
  public Class<? extends MasterObserver>[] getMasterObserverClasses() {
    return MASTER_OBSERVER_CLASSES.getArray(this);
  }

  /**
   * Get array of WorkerObserver classes set in configuration.
   *
   * @return array of WorkerObserver classes.
   */
  public Class<? extends WorkerObserver>[] getWorkerObserverClasses() {
    return WORKER_OBSERVER_CLASSES.getArray(this);
  }

  /**
   * Whether to track, print, and aggregate metrics.
   *
   * @return true if metrics are enabled, false otherwise (default)
   */
  public boolean metricsEnabled() {
    return METRICS_ENABLE.isTrue(this);
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
   * Is this a "pure YARN" Giraph job, or is a MapReduce layer (v1 or v2)
   * actually managing our cluster nodes, i.e. each task is a Mapper.
   * @return TRUE if this is a pure YARN job.
   */
  public boolean isPureYarnJob() {
    return IS_PURE_YARN_JOB.get(this);
  }

  /**
   * Jars required in "Pure YARN" jobs (names only, no paths) should
   * be listed here in full, including Giraph framework jar(s).
   * @return the comma-separated list of jar names for export to cluster.
   */
  public String getYarnLibJars() {
    return GIRAPH_YARN_LIBJARS.get(this);
  }

  /**
   * Populate jar list for Pure YARN jobs.
   * @param jarList a comma-separated list of jar names
   */
  public void setYarnLibJars(String jarList) {
    GIRAPH_YARN_LIBJARS.set(this, jarList);
  }

  /**
   * Get heap size (in MB) for each task in our Giraph job run,
   * assuming this job will run on the "pure YARN" profile.
   * @return the heap size for all tasks, in MB
   */
  public int getYarnTaskHeapMb() {
    return GIRAPH_YARN_TASK_HEAP_MB.get(this);
  }

  /**
   * Set heap size for Giraph tasks in our job run, assuming
   * the job will run on the "pure YARN" profile.
   * @param heapMb the heap size for all tasks
   */
  public void setYarnTaskHeapMb(int heapMb) {
    GIRAPH_YARN_TASK_HEAP_MB.set(this, heapMb);
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
    return LOG_LEVEL.get(this);
  }

  /**
   * Use the log thread layout option?
   *
   * @return True if use the log thread layout option, false otherwise
   */
  public boolean useLogThreadLayout() {
    return LOG_THREAD_LAYOUT.get(this);
  }

  /**
   * is this job run a local test?
   * @return the test status as recorded in the Configuration
   */
  public boolean getLocalTestMode() {
    return LOCAL_TEST_MODE.get(this);
  }

  /**
   * Flag this job as a local test run.
   * @param flag the test status for this job
   */
  public void setLocalTestMode(boolean flag) {
    LOCAL_TEST_MODE.set(this, flag);
  }

  /**
   * The number of server tasks in our ZK quorum for
   * this job run.
   * @return the number of ZK servers in the quorum
   */
  public int getZooKeeperServerCount() {
    return ZOOKEEPER_SERVER_COUNT.get(this);
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
    return ZOOKEEPER_SESSION_TIMEOUT.get(this);
  }

  public int getZookeeperOpsMaxAttempts() {
    return ZOOKEEPER_OPS_MAX_ATTEMPTS.get(this);
  }

  public int getZookeeperOpsRetryWaitMsecs() {
    return ZOOKEEPER_OPS_RETRY_WAIT_MSECS.get(this);
  }

  public boolean getNettyServerUseExecutionHandler() {
    return NETTY_SERVER_USE_EXECUTION_HANDLER.get(this);
  }

  public int getNettyServerThreads() {
    return NETTY_SERVER_THREADS.get(this);
  }

  public int getNettyServerExecutionThreads() {
    return NETTY_SERVER_EXECUTION_THREADS.get(this);
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
    return ZOOKEEPER_CONNECTION_ATTEMPTS.get(this);
  }

  public int getZooKeeperMinSessionTimeout() {
    return ZOOKEEPER_MIN_SESSION_TIMEOUT.get(this);
  }

  public int getZooKeeperMaxSessionTimeout() {
    return ZOOKEEPER_MAX_SESSION_TIMEOUT.get(this);
  }

  public boolean getZooKeeperForceSync() {
    return ZOOKEEPER_FORCE_SYNC.get(this);
  }

  public boolean getZooKeeperSkipAcl() {
    return ZOOKEEPER_SKIP_ACL.get(this);
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
    return AUTHENTICATE.get(this);
  }

  /**
   * Set the number of compute threads
   *
   * @param numComputeThreads Number of compute threads to use
   */
  public void setNumComputeThreads(int numComputeThreads) {
    NUM_COMPUTE_THREADS.set(this, numComputeThreads);
  }

  public int getNumComputeThreads() {
    return NUM_COMPUTE_THREADS.get(this);
  }

  /**
   * Set the number of input split threads
   *
   * @param numInputSplitsThreads Number of input split threads to use
   */
  public void setNumInputSplitsThreads(int numInputSplitsThreads) {
    NUM_INPUT_THREADS.set(this, numInputSplitsThreads);
  }

  public int getNumInputSplitsThreads() {
    return NUM_INPUT_THREADS.get(this);
  }

  public long getInputSplitMaxVertices() {
    return INPUT_SPLIT_MAX_VERTICES.get(this);
  }

  public long getInputSplitMaxEdges() {
    return INPUT_SPLIT_MAX_EDGES.get(this);
  }

  /**
   * Set whether to use unsafe serialization
   *
   * @param useUnsafeSerialization If true, use unsafe serialization
   */
  public void useUnsafeSerialization(boolean useUnsafeSerialization) {
    USE_UNSAFE_SERIALIZATION.set(this, useUnsafeSerialization);
  }

  /**
   * Use message size encoding?  This feature may help with complex message
   * objects.
   *
   * @return Whether to use message size encoding
   */
  public boolean useMessageSizeEncoding() {
    return USE_MESSAGE_SIZE_ENCODING.get(this);
  }

  /**
   * Set the checkpoint frequeuncy of how many supersteps to wait before
   * checkpointing
   *
   * @param checkpointFrequency How often to checkpoint (0 means never)
   */
  public void setCheckpointFrequency(int checkpointFrequency) {
    CHECKPOINT_FREQUENCY.set(this, checkpointFrequency);
  }

  /**
   * Get the checkpoint frequeuncy of how many supersteps to wait
   * before checkpointing
   *
   * @return Checkpoint frequency (0 means never)
   */
  public int getCheckpointFrequency() {
    return CHECKPOINT_FREQUENCY.get(this);
  }

  /**
   * Check if checkpointing is used
   *
   * @return True iff checkpointing is used
   */
  public boolean useCheckpointing() {
    return getCheckpointFrequency() != 0;
  }

  /**
   * Set the max task attempts
   *
   * @param maxTaskAttempts Max task attempts to use
   */
  public void setMaxTaskAttempts(int maxTaskAttempts) {
    MAX_TASK_ATTEMPTS.set(this, maxTaskAttempts);
  }

  /**
   * Get the max task attempts
   *
   * @return Max task attempts or -1, if not set
   */
  public int getMaxTaskAttempts() {
    return MAX_TASK_ATTEMPTS.get(this);
  }

  /**
   * Get the number of milliseconds to wait for an event before continuing on
   *
   * @return Number of milliseconds to wait for an event before continuing on
   */
  public int getEventWaitMsecs() {
    return EVENT_WAIT_MSECS.get(this);
  }

  /**
   * Set the number of milliseconds to wait for an event before continuing on
   *
   * @param eventWaitMsecs Number of milliseconds to wait for an event before
   *                       continuing on
   */
  public void setEventWaitMsecs(int eventWaitMsecs) {
    EVENT_WAIT_MSECS.set(this, eventWaitMsecs);
  }

  /**
   * Get the maximum milliseconds to wait before giving up trying to get the
   * minimum number of workers before a superstep.
   *
   * @return Maximum milliseconds to wait before giving up trying to get the
   *         minimum number of workers before a superstep
   */
  public int getMaxMasterSuperstepWaitMsecs() {
    return MAX_MASTER_SUPERSTEP_WAIT_MSECS.get(this);
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
    MAX_MASTER_SUPERSTEP_WAIT_MSECS.set(this, maxMasterSuperstepWaitMsecs);
  }

  /**
   * Check environment for Hadoop security token location in case we are
   * executing the Giraph job on a MRv1 Hadoop cluster.
   */
  public void configureHadoopSecurity() {
    String hadoopTokenFilePath = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
    if (hadoopTokenFilePath != null) {
      set("mapreduce.job.credentials.binary", hadoopTokenFilePath);
    }
  }

  /**
   * Check if we want to prioritize input splits which reside on the host.
   *
   * @return True iff we want to use input split locality
   */
  public boolean useInputSplitLocality() {
    return USE_INPUT_SPLIT_LOCALITY.get(this);
  }

  /**
   * Check if we can reuse incoming edge objects.
   *
   * @return True iff we can reuse incoming edge objects.
   */
  public boolean reuseIncomingEdgeObjects() {
    return GiraphConstants.REUSE_INCOMING_EDGE_OBJECTS.get(this);
  }

  /**
   * Get the local hostname on the given interface.
   *
   * @return The local hostname
   * @throws UnknownHostException
   */
  public String getLocalHostname() throws UnknownHostException {
    return DNS.getDefaultHost(
        GiraphConstants.DNS_INTERFACE.get(this),
        GiraphConstants.DNS_NAMESERVER.get(this));
  }

  /**
   * Set the maximum number of supersteps of this application.  After this
   * many supersteps are executed, the application will shutdown.
   *
   * @param maxNumberOfSupersteps Maximum number of supersteps
   */
  public void setMaxNumberOfSupersteps(int maxNumberOfSupersteps) {
    MAX_NUMBER_OF_SUPERSTEPS.set(this, maxNumberOfSupersteps);
  }

  /**
   * Get the maximum number of supersteps of this application.  After this
   * many supersteps are executed, the application will shutdown.
   *
   * @return Maximum number of supersteps
   */
  public int getMaxNumberOfSupersteps() {
    return MAX_NUMBER_OF_SUPERSTEPS.get(this);
  }

  /**
   * Whether the application with change or not the graph topology.
   *
   * @return true if the graph is static, false otherwise.
   */
  public boolean isStaticGraph() {
    return STATIC_GRAPH.isTrue(this);
  }
}
