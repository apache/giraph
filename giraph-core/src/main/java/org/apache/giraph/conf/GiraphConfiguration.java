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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.bsp.checkpoints.CheckpointSupportedChecker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReuseObjectsOutEdges;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.MapperObserver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.job.GiraphJobRetryChecker;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.ReusesObjectsPartition;
import org.apache.giraph.utils.GcObserver;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.net.DNS;

/**
 * Adds user methods specific to Giraph.  This will be put into an
 * ImmutableClassesGiraphConfiguration that provides the configuration plus
 * the immutable classes.
 *
 * Keeps track of parameters which were set so it easily set them in another
 * copy of configuration.
 */
public class GiraphConfiguration extends Configuration
    implements GiraphConstants {
  /** ByteBufAllocator to be used by netty */
  private ByteBufAllocator nettyBufferAllocator = null;

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
   * Get name of computation being run. We leave this up to the
   * {@link ComputationFactory} to decide what to return.
   *
   * @return Name of computation being run
   */
  public String getComputationName() {
    ComputationFactory compFactory = ReflectionUtils.newInstance(
        getComputationFactoryClass());
    return compFactory.computationName(this);
  }

  /**
   * Get the user's subclassed {@link ComputationFactory}
   *
   * @return User's computation factory class
   */
  public Class<? extends ComputationFactory> getComputationFactoryClass() {
    return COMPUTATION_FACTORY_CLASS.get(this);
  }

  /**
   * Get the user's subclassed {@link Computation}
   *
   * @return User's computation class
   */
  public Class<? extends Computation> getComputationClass() {
    return COMPUTATION_CLASS.get(this);
  }

  /**
   * Set the computation class (required)
   *
   * @param computationClass Runs vertex computation
   */
  public void setComputationClass(
      Class<? extends Computation> computationClass) {
    COMPUTATION_CLASS.set(this, computationClass);
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
   * Set the edge input filter class
   *
   * @param edgeFilterClass class to use
   */
  public void setEdgeInputFilterClass(
      Class<? extends EdgeInputFilter> edgeFilterClass) {
    EDGE_INPUT_FILTER_CLASS.set(this, edgeFilterClass);
  }

  /**
   * Set the vertex input filter class
   *
   * @param vertexFilterClass class to use
   */
  public void setVertexInputFilterClass(
      Class<? extends VertexInputFilter> vertexFilterClass) {
    VERTEX_INPUT_FILTER_CLASS.set(this, vertexFilterClass);
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
   * Set the vertex implementation class
   *
   * @param vertexClass class of the vertex implementation
   */
  public final void setVertexClass(Class<? extends Vertex> vertexClass) {
    VERTEX_CLASS.set(this, vertexClass);
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
  public void setVertexInputFormatClass(
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
  public void setEdgeInputFormatClass(
      Class<? extends EdgeInputFormat> edgeInputFormatClass) {
    EDGE_INPUT_FORMAT_CLASS.set(this, edgeInputFormatClass);
  }

  /**
   * Set the mapping input format class (optional)
   *
   * @param mappingInputFormatClass Determines how mappings are input
   */
  public void setMappingInputFormatClass(
    Class<? extends MappingInputFormat> mappingInputFormatClass) {
    MAPPING_INPUT_FORMAT_CLASS.set(this, mappingInputFormatClass);
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
   * Add a MapperObserver class (optional)
   *
   * @param mapperObserverClass MapperObserver class to add.
   */
  public final void addMapperObserverClass(
      Class<? extends MapperObserver> mapperObserverClass) {
    MAPPER_OBSERVER_CLASSES.add(this, mapperObserverClass);
  }

  /**
   * Add a GcObserver class (optional)
   *
   * @param gcObserverClass GcObserver class to add.
   */
  public final void addGcObserverClass(
      Class<? extends GcObserver> gcObserverClass) {
    GC_OBSERVER_CLASSES.add(this, gcObserverClass);
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
   * Get job retry checker class
   *
   * @return GiraphJobRetryChecker class set.
   */
  public Class<? extends GiraphJobRetryChecker> getJobRetryCheckerClass() {
    return JOB_RETRY_CHECKER_CLASS.get(this);
  }

  /**
   * Set job retry checker class
   *
   * @param klass GiraphJobRetryChecker class to set.
   */
  public void setJobRetryCheckerClass(
      Class<? extends GiraphJobRetryChecker> klass) {
    JOB_RETRY_CHECKER_CLASS.set(this, klass);
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
   * Check whether to enable heap memory supervisor thread
   *
   * @return true if jmap dumper is reactively enabled
   */
  public boolean isReactiveJmapHistogramDumpEnabled() {
    return REACTIVE_JMAP_ENABLE.get(this);
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
   * Does the job have a {@link EdgeOutputFormat} subdir?
   *
   * @return True iff a {@link EdgeOutputFormat} subdir has been specified.
   */
  public boolean hasVertexOutputFormatSubdir() {
    return !VERTEX_OUTPUT_FORMAT_SUBDIR.get(this).isEmpty();
  }

  /**
   * Set the vertex output format path
   *
   * @param path path where the verteces will be written
   */
  public final void setVertexOutputFormatSubdir(String path) {
    VERTEX_OUTPUT_FORMAT_SUBDIR.set(this, path);
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
   * Does the job have a {@link EdgeOutputFormat}?
   *
   * @return True iff a {@link EdgeOutputFormat} has been specified.
   */
  public boolean hasEdgeOutputFormat() {
    return EDGE_OUTPUT_FORMAT_CLASS.get(this) != null;
  }

  /**
   * Set the edge output format class (optional)
   *
   * @param edgeOutputFormatClass Determines how graph is output
   */
  public final void setEdgeOutputFormatClass(
      Class<? extends EdgeOutputFormat> edgeOutputFormatClass) {
    EDGE_OUTPUT_FORMAT_CLASS.set(this, edgeOutputFormatClass);
  }

  /**
   * Does the job have a {@link EdgeOutputFormat} subdir?
   *
   * @return True iff a {@link EdgeOutputFormat} subdir has been specified.
   */
  public boolean hasEdgeOutputFormatSubdir() {
    return !EDGE_OUTPUT_FORMAT_SUBDIR.get(this).isEmpty();
  }

  /**
   * Set the edge output format path
   *
   * @param path path where the edges will be written
   */
  public final void setEdgeOutputFormatSubdir(String path) {
    EDGE_OUTPUT_FORMAT_SUBDIR.set(this, path);
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
   * Set the message combiner class (optional)
   *
   * @param messageCombinerClass Determines how vertex messages are combined
   */
  public void setMessageCombinerClass(
      Class<? extends MessageCombiner> messageCombinerClass) {
    MESSAGE_COMBINER_CLASS.set(this, messageCombinerClass);
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
   * Set the vertex value combiner class (optional)
   *
   * @param vertexValueCombinerClass Determines how vertices are combined
   */
  public final void setVertexValueCombinerClass(
      Class<? extends VertexValueCombiner> vertexValueCombinerClass) {
    VERTEX_VALUE_COMBINER_CLASS.set(this, vertexValueCombinerClass);
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
   * How many mappers is job asking for, taking into account whether master
   * is running on the same mapper as worker or not
   *
   * @return How many mappers is job asking for
   */
  public final int getMaxMappers() {
    return getMaxWorkers() + (SPLIT_MASTER_WORKER.get(this) ? 1 : 0);
  }

  /**
   * Utilize an existing ZooKeeper service.  If this is not set, ZooKeeper
   * will be dynamically started by Giraph for this job.
   *
   * @param serverList Comma separated list of servers and ports
   *        (i.e. zk1:2221,zk2:2221)
   */
  public final void setZooKeeperConfiguration(String serverList) {
    ZOOKEEPER_LIST.set(this, serverList);
  }

  /**
   * Getter for SPLIT_MASTER_WORKER flag.
   *
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
   * Get array of MapperObserver classes set in configuration.
   *
   * @return array of MapperObserver classes.
   */
  public Class<? extends MapperObserver>[] getMapperObserverClasses() {
    return MAPPER_OBSERVER_CLASSES.getArray(this);
  }

  /**
   * Get array of GcObserver classes set in configuration.
   *
   * @return array of GcObserver classes.
   */
  public Class<? extends GcObserver>[] getGcObserverClasses() {
    return GC_OBSERVER_CLASSES.getArray(this);
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
   *
   * @return TRUE if this is a pure YARN job.
   */
  public boolean isPureYarnJob() {
    return IS_PURE_YARN_JOB.get(this);
  }

  /**
   * Jars required in "Pure YARN" jobs (names only, no paths) should
   * be listed here in full, including Giraph framework jar(s).
   *
   * @return the comma-separated list of jar names for export to cluster.
   */
  public String getYarnLibJars() {
    return GIRAPH_YARN_LIBJARS.get(this);
  }

  /**
   * Populate jar list for Pure YARN jobs.
   *
   * @param jarList a comma-separated list of jar names
   */
  public void setYarnLibJars(String jarList) {
    GIRAPH_YARN_LIBJARS.set(this, jarList);
  }

  /**
   * Get heap size (in MB) for each task in our Giraph job run,
   * assuming this job will run on the "pure YARN" profile.
   *
   * @return the heap size for all tasks, in MB
   */
  public int getYarnTaskHeapMb() {
    return GIRAPH_YARN_TASK_HEAP_MB.get(this);
  }

  /**
   * Set heap size for Giraph tasks in our job run, assuming
   * the job will run on the "pure YARN" profile.
   *
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
    return ZOOKEEPER_LIST.get(this);
  }

  /**
   * Set the ZooKeeper list to the provided list. This method is used when the
   * ZooKeeper is started internally and will set the zkIsExternal option to
   * false as well.
   *
   * @param zkList list of strings, comma separated of zookeeper servers
   */
  public void setZookeeperList(String zkList) {
    ZOOKEEPER_LIST.set(this, zkList);
    ZOOKEEPER_IS_EXTERNAL.set(this, false);
  }

  /**
   * Was ZooKeeper provided externally?
   *
   * @return true iff was zookeeper is external
   */
  public boolean isZookeeperExternal() {
    return ZOOKEEPER_IS_EXTERNAL.get(this);
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
   *
   * @return the test status as recorded in the Configuration
   */
  public boolean getLocalTestMode() {
    return LOCAL_TEST_MODE.get(this);
  }

  /**
   * Flag this job as a local test run.
   *
   * @param flag the test status for this job
   */
  public void setLocalTestMode(boolean flag) {
    LOCAL_TEST_MODE.set(this, flag);
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

  /**
   * Used by netty client and server to create ByteBufAllocator
   *
   * @return ByteBufAllocator
   */
  public ByteBufAllocator getNettyAllocator() {
    if (nettyBufferAllocator == null) {
      if (NETTY_USE_POOLED_ALLOCATOR.get(this)) { // Use pooled allocator
        nettyBufferAllocator = new PooledByteBufAllocator(
          NETTY_USE_DIRECT_MEMORY.get(this));
      } else { // Use un-pooled allocator
        // Note: Current default settings create un-pooled heap allocator
        nettyBufferAllocator = new UnpooledByteBufAllocator(
            NETTY_USE_DIRECT_MEMORY.get(this));
      }
    }
    return nettyBufferAllocator;
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
   * Set runtime checkpoint support checker.
   * The instance of this class will have to decide whether
   * checkpointing is allowed on current superstep.
   *
   * @param clazz checkpoint supported checker class
   */
  public void setCheckpointSupportedChecker(
      Class<? extends CheckpointSupportedChecker> clazz) {
    GiraphConstants.CHECKPOINT_SUPPORTED_CHECKER.set(this, clazz);
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
   * Get the local hostname on the given interface.
   *
   * @return The local hostname
   * @throws UnknownHostException IP address of a host could not be determined
   */
  public String getLocalHostname() throws UnknownHostException {
    return DNS.getDefaultHost(
        GiraphConstants.DNS_INTERFACE.get(this),
        GiraphConstants.DNS_NAMESERVER.get(this)).toLowerCase();
  }

  /**
   * Return local host name by default. Or local host IP if preferIP
   * option is set.
   * @return local host name or IP
   * @throws UnknownHostException IP address of a host could not be determined
   */
  public String getLocalHostOrIp() throws UnknownHostException {
    if (GiraphConstants.PREFER_IP_ADDRESSES.get(this)) {
      return InetAddress.getLocalHost().getHostAddress();
    }
    return getLocalHostname();
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
   * Get the output directory to write YourKit snapshots to
   *
   * @param context Map context
   * @return output directory
   */
  public String getYourKitOutputDir(Mapper.Context context) {
    final String cacheKey = "giraph.yourkit.outputDirCached";
    String outputDir = get(cacheKey);
    if (outputDir == null) {
      outputDir = getStringVars(YOURKIT_OUTPUT_DIR, YOURKIT_OUTPUT_DIR_DEFAULT,
          context);
      set(cacheKey, outputDir);
    }
    return outputDir;
  }

  /**
   * Get string, replacing variables in the output.
   *
   * %JOB_ID% =&gt; job id
   * %TASK_ID% =&gt; task id
   * %USER% =&gt; owning user name
   *
   * @param key name of key to lookup
   * @param context mapper context
   * @return value for key, with variables expanded
   */
  public String getStringVars(String key, Mapper.Context context) {
    return getStringVars(key, null, context);
  }

  /**
   * Get string, replacing variables in the output.
   *
   * %JOB_ID% =&gt; job id
   * %TASK_ID% =&gt; task id
   * %USER% =&gt; owning user name
   *
   * @param key name of key to lookup
   * @param defaultValue value to return if no mapping exists. This can also
   *                     have variables, which will be substituted.
   * @param context mapper context
   * @return value for key, with variables expanded
   */
  public String getStringVars(String key, String defaultValue,
                              Mapper.Context context) {
    String value = get(key);
    if (value == null) {
      if (defaultValue == null) {
        return null;
      }
      value = defaultValue;
    }
    value = value.replace("%JOB_ID%", context.getJobID().toString());
    value = value.replace("%TASK_ID%", context.getTaskAttemptID().toString());
    value = value.replace("%USER%", get("user.name", "unknown_user"));
    return value;
  }

  /**
   * Get option whether to create a source vertex present only in edge input
   *
   * @return CREATE_EDGE_SOURCE_VERTICES option
   */
  public boolean getCreateSourceVertex() {
    return CREATE_EDGE_SOURCE_VERTICES.get(this);
  }

  /**
   * set option whether to create a source vertex present only in edge input
   * @param createVertex create source vertex option
   */
  public void setCreateSourceVertex(boolean createVertex) {
    CREATE_EDGE_SOURCE_VERTICES.set(this, createVertex);
  }

  /**
   * Get the maximum timeout (in milliseconds) for waiting for all tasks
   * to complete after the job is done.
   *
   * @return Wait task done timeout in milliseconds.
   */
  public int getWaitTaskDoneTimeoutMs() {
    return WAIT_TASK_DONE_TIMEOUT_MS.get(this);
  }

  /**
   * Set the maximum timeout (in milliseconds) for waiting for all tasks
   * to complete after the job is done.
   *
   * @param ms Milliseconds to set
   */
  public void setWaitTaskDoneTimeoutMs(int ms) {
    WAIT_TASK_DONE_TIMEOUT_MS.set(this, ms);
  }

  /**
   * Check whether to track job progress on client or not
   *
   * @return True if job progress should be tracked on client
   */
  public boolean trackJobProgressOnClient() {
    return TRACK_JOB_PROGRESS_ON_CLIENT.get(this);
  }

  /**
   * @return Number of retries when creating an HDFS file before failing.
   */
  public int getHdfsFileCreationRetries() {
    return HDFS_FILE_CREATION_RETRIES.get(this);
  }

  /**
   * @return Milliseconds to wait before retrying an HDFS file creation
   *         operation.
   */
  public int getHdfsFileCreationRetryWaitMs() {
    return HDFS_FILE_CREATION_RETRY_WAIT_MS.get(this);
  }
}
