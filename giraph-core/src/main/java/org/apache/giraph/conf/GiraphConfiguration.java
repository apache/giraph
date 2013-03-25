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
import org.apache.giraph.edge.VertexEdges;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.job.DefaultJobObserver;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.graph.Vertex;
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
   * Set the vertex class (required)
   *
   * @param vertexClass Runs vertex computation
   */
  public final void setVertexClass(
      Class<? extends Vertex> vertexClass) {
    setClass(VERTEX_CLASS, vertexClass, Vertex.class);
  }

  /**
   * Set the vertex value factory class
   *
   * @param vertexValueFactoryClass Creates default vertex values
   */
  public final void setVertexValueFactoryClass(
      Class<? extends VertexValueFactory> vertexValueFactoryClass) {
    setClass(VERTEX_VALUE_FACTORY_CLASS, vertexValueFactoryClass,
        VertexValueFactory.class);
  }

  /**
   * Set the vertex edges class
   *
   * @param vertexEdgesClass Determines the way edges are stored
   */
  public final void setVertexEdgesClass(
      Class<? extends VertexEdges> vertexEdgesClass) {
    setClass(VERTEX_EDGES_CLASS, vertexEdgesClass, VertexEdges.class);
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
   * Add a WorkerObserver class (optional)
   *
   * @param workerObserverClass WorkerObserver class to add.
   */
  public final void addWorkerObserverClass(
      Class<? extends WorkerObserver> workerObserverClass) {
    addToClasses(WORKER_OBSERVER_CLASSES, workerObserverClass,
        WorkerObserver.class);
  }

  /**
   * Get job observer class
   *
   * @return GiraphJobObserver class set.
   */
  public Class<? extends GiraphJobObserver> getJobObserverClass() {
    return getClass(JOB_OBSERVER_CLASS, DefaultJobObserver.class,
        GiraphJobObserver.class);
  }

  /**
   * Set job observer class
   *
   * @param klass GiraphJobObserver class to set.
   */
  public void setJobObserverClass(Class<? extends GiraphJobObserver> klass) {
    setClass(JOB_OBSERVER_CLASS, klass, GiraphJobObserver.class);
  }

  /**
   * Check whether to enable jmap dumping thread.
   *
   * @return true if jmap dumper is enabled.
   */
  public boolean isJMapHistogramDumpEnabled() {
    return getBoolean(JMAP_ENABLE, JMAP_ENABLE_DEFAULT);
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
    setClass(VERTEX_COMBINER_CLASS, vertexCombinerClass, Combiner.class);
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
   * Set the partition context class (optional)
   *
   * @param partitionContextClass Determines what code is executed for each
   *        partition before and after each superstep
   */
  public final void setPartitionContextClass(
      Class<? extends PartitionContext> partitionContextClass) {
    setClass(PARTITION_CONTEXT_CLASS, partitionContextClass,
        PartitionContext.class);
  }

  /**
   * Set the worker context class (optional)
   *
   * @param workerContextClass Determines what code is executed on a each
   *        worker before and after each superstep and computation
   */
  public final void setWorkerContextClass(
      Class<? extends WorkerContext> workerContextClass) {
    setClass(WORKER_CONTEXT_CLASS, workerContextClass, WorkerContext.class);
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
   * Get array of WorkerObserver classes set in configuration.
   *
   * @return array of WorkerObserver classes.
   */
  public Class<? extends WorkerObserver>[] getWorkerObserverClasses() {
    return getClassesOfType(WORKER_OBSERVER_CLASSES, WorkerObserver.class);
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
    return getInt(ZOOKEEPER_SERVER_COUNT, ZOOKEEPER_SERVER_COUNT_DEFAULT);
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
    return getInt(ZOOKEEPER_SESSION_TIMEOUT, ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
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
    return getBoolean(GiraphConstants.USE_INPUT_SPLIT_LOCALITY,
        GiraphConstants.USE_INPUT_SPLIT_LOCALITY_DEFAULT);
  }

  /**
   * Check if we can reuse incoming edge objects.
   *
   * @return True iff we can reuse incoming edge objects.
   */
  public boolean reuseIncomingEdgeObjects() {
    return getBoolean(GiraphConstants.REUSE_INCOMING_EDGE_OBJECTS,
        GiraphConstants.REUSE_INCOMING_EDGE_OBJECTS_DEFAULT);
  }

  /**
   * Get the local hostname on the given interface.
   *
   * @return The local hostname
   * @throws UnknownHostException
   */
  public String getLocalHostname() throws UnknownHostException {
    return DNS.getDefaultHost(
        get(GiraphConstants.DNS_INTERFACE, "default"),
        get(GiraphConstants.DNS_NAMESERVER, "default"));
  }

  /**
   * Set the maximum number of supersteps of this application.  After this
   * many supersteps are executed, the application will shutdown.
   *
   * @param maxNumberOfSupersteps Maximum number of supersteps
   */
  public void setMaxNumberOfSupersteps(int maxNumberOfSupersteps) {
    setInt(MAX_NUMBER_OF_SUPERSTEPS, maxNumberOfSupersteps);
  }

  /**
   * Get the maximum number of supersteps of this application.  After this
   * many supersteps are executed, the application will shutdown.
   *
   * @return Maximum number of supersteps
   */
  public int getMaxNumberOfSupersteps() {
    return getInt(MAX_NUMBER_OF_SUPERSTEPS, MAX_NUMBER_OF_SUPERSTEPS_DEFAULT);
  }
}
