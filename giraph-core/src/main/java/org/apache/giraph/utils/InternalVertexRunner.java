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

package org.apache.giraph.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.FileOutputFormatUtil;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.zk.InProcessZooKeeperRunner;
import org.apache.giraph.zk.ZookeeperConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A base class for running internal tests on a vertex
 *
 * Extending classes only have to invoke the run() method to test their vertex.
 * All data is written to a local tmp directory that is removed afterwards.
 * A local zookeeper instance is started in an extra thread and
 * shutdown at the end.
 *
 * Heavily inspired from Apache Mahout's MahoutTestCase
 */
@SuppressWarnings("unchecked")
public class InternalVertexRunner {

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(InternalVertexRunner.class);

  /** Don't construct */
  private InternalVertexRunner() { }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from and
   * writing to a temporary folder on local disk. Will start its own zookeeper
   * instance.
   *
   * @param conf GiraphClasses specifying which types to use
   * @param vertexInputData linewise vertex input data
   * @return linewise output data, or null if job fails
   * @throws Exception if anything goes wrong
   */
  public static Iterable<String> run(
      GiraphConfiguration conf,
      String[] vertexInputData) throws Exception {
    return run(conf, vertexInputData, null);
  }

  /**
   * Run the ZooKeeper in-process and the job.
   *
   * @param zookeeperConfig Quorum peer configuration
   * @param giraphJob Giraph job to run
   * @return True if successful, false otherwise
   */
  private static boolean runZooKeeperAndJob(
      final ZookeeperConfig zookeeperConfig,
      GiraphJob giraphJob) throws IOException {
    final InProcessZooKeeperRunner.ZooKeeperServerRunner zookeeper =
        new InProcessZooKeeperRunner.ZooKeeperServerRunner();

    int port = zookeeper.start(zookeeperConfig);

    LOG.info("Started test zookeeper on port " + port);
    GiraphConstants.ZOOKEEPER_LIST.set(giraphJob.getConfiguration(),
        "localhost:" + port);
    try {
      return giraphJob.run(true);
    } catch (InterruptedException |
        ClassNotFoundException | IOException e) {
      LOG.error("runZooKeeperAndJob: Got exception on running", e);
    } finally {
      zookeeper.stop();
    }

    return false;
  }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from and
   * writing to a temporary folder on local disk. Will start its own zookeeper
   * instance.
   *
   *
   * @param conf GiraphClasses specifying which types to use
   * @param vertexInputData linewise vertex input data
   * @param edgeInputData linewise edge input data
   * @return linewise output data, or null if job fails
   * @throws Exception if anything goes wrong
   */
  public static Iterable<String> run(
      GiraphConfiguration conf,
      String[] vertexInputData,
      String[] edgeInputData) throws Exception {
    // Prepare input file, output folder and temporary folders
    File tmpDir = FileUtils.createTestDir(conf.getComputationName());
    try {
      return run(conf, vertexInputData, edgeInputData, null, tmpDir);
    } finally {
      FileUtils.delete(tmpDir);
    }
  }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from and
   * writing to a temporary folder on local disk. Will start its own zookeeper
   * instance.
   *
   *
   * @param conf GiraphClasses specifying which types to use
   * @param vertexInputData linewise vertex input data
   * @param edgeInputData linewise edge input data
   * @param checkpointsDir if set, will use this folder
   *                          for storing checkpoints.
   * @param tmpDir file path for storing temporary files.
   * @return linewise output data, or null if job fails
   * @throws Exception if anything goes wrong
   */
  public static Iterable<String> run(
      GiraphConfiguration conf,
      String[] vertexInputData,
      String[] edgeInputData,
      String checkpointsDir,
      File tmpDir) throws Exception {
    File vertexInputFile = null;
    File edgeInputFile = null;
    if (conf.hasVertexInputFormat()) {
      vertexInputFile = FileUtils.createTempFile(tmpDir, "vertices.txt");
    }
    if (conf.hasEdgeInputFormat()) {
      edgeInputFile = FileUtils.createTempFile(tmpDir, "edges.txt");
    }

    File outputDir = FileUtils.createTempDir(tmpDir, "output");
    File zkDir = FileUtils.createTempDir(tmpDir, "_bspZooKeeper");
    File zkMgrDir = FileUtils.createTempDir(tmpDir, "_defaultZkManagerDir");
    File mrLocalDir = FileUtils.createTempDir(tmpDir, "_mapred");
    // Write input data to disk
    if (conf.hasVertexInputFormat()) {
      FileUtils.writeLines(vertexInputFile, vertexInputData);
    }
    if (conf.hasEdgeInputFormat()) {
      FileUtils.writeLines(edgeInputFile, edgeInputData);
    }

    conf.setWorkerConfiguration(1, 1, 100.0f);
    GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
    GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
    conf.setIfUnset("mapred.job.tracker", "local");
    conf.setIfUnset("mapred.local.dir", mrLocalDir.toString());

    conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
    GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
        zkMgrDir.toString());

    if (checkpointsDir == null) {
      checkpointsDir = FileUtils.createTempDir(
          tmpDir, "_checkpoints").toString();
    }
    GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkpointsDir);

    // Create and configure the job to run the vertex
    GiraphJob job = new GiraphJob(conf, conf.getComputationName());

    Job internalJob = job.getInternalJob();
    if (conf.hasVertexInputFormat()) {
      GiraphFileInputFormat.setVertexInputPath(internalJob.getConfiguration(),
          new Path(vertexInputFile.toString()));
    }
    if (conf.hasEdgeInputFormat()) {
      GiraphFileInputFormat.setEdgeInputPath(internalJob.getConfiguration(),
          new Path(edgeInputFile.toString()));
    }
    FileOutputFormatUtil.setOutputPath(job.getInternalJob(),
        new Path(outputDir.toString()));

    // Configure a local zookeeper instance
    ZookeeperConfig qpConfig = configLocalZooKeeper(zkDir);

    boolean success = runZooKeeperAndJob(qpConfig, job);
    if (!success) {
      return null;
    }

    File outFile = new File(outputDir, "part-m-00000");
    if (conf.hasVertexOutputFormat() && outFile.canRead()) {
      return Files.readLines(outFile, Charsets.UTF_8);
    } else {
      return ImmutableList.of();
    }

  }

  /**
   * Attempts to run the vertex internally in the current JVM,
   * reading from an in-memory graph. Will start its own zookeeper
   * instance.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf GiraphClasses specifying which types to use
   * @param graph input graph
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
      V extends Writable,
      E extends Writable> void run(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph) throws Exception {
    // Prepare temporary folders
    File tmpDir = FileUtils.createTestDir(conf.getComputationName());
    try {
      run(conf, graph, tmpDir, null);
    } finally {
      FileUtils.delete(tmpDir);
    }
  }

  /**
   * Attempts to run the vertex internally in the current JVM,
   * reading from an in-memory graph. Will start its own zookeeper
   * instance.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf GiraphClasses specifying which types to use
   * @param graph input graph
   * @param tmpDir file path for storing temporary files.
   * @param checkpointsDir if set, will use this folder
   *                          for storing checkpoints.
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
      V extends Writable,
      E extends Writable> void run(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph,
      File tmpDir,
      String checkpointsDir) throws Exception {
    File zkDir = FileUtils.createTempDir(tmpDir, "_bspZooKeeper");
    File zkMgrDir = FileUtils.createTempDir(tmpDir, "_defaultZkManagerDir");
    File mrLocalDir = FileUtils.createTempDir(tmpDir, "_mapred");

    if (checkpointsDir == null) {
      checkpointsDir = FileUtils.
          createTempDir(tmpDir, "_checkpoints").toString();
    }

    conf.setVertexInputFormatClass(InMemoryVertexInputFormat.class);

    // Create and configure the job to run the vertex
    GiraphJob job = new GiraphJob(conf, conf.getComputationName());

    InMemoryVertexInputFormat.setGraph(graph);

    conf.setWorkerConfiguration(1, 1, 100.0f);
    GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
    GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
    GiraphConstants.ZOOKEEPER_SERVER_PORT.set(conf, 0);
    conf.setIfUnset("mapred.job.tracker", "local");
    conf.setIfUnset("mapred.local.dir", mrLocalDir.toString());

    conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
    GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
        zkMgrDir.toString());
    GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkpointsDir);

    runZooKeeperAndJob(configLocalZooKeeper(zkDir), job);
  }

  /**
   * Attempts to run the vertex internally in the current JVM, reading and
   * writing to an in-memory graph. Will start its own zookeeper
   * instance.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf GiraphClasses specifying which types to use
   * @param graph input graph
   * @return Output graph
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
      V extends Writable,
      E extends Writable> TestGraph<I, V, E> runWithInMemoryOutput(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph) throws Exception {
    // Prepare temporary folders
    File tmpDir = FileUtils.createTestDir(conf.getComputationName());
    try {
      return runWithInMemoryOutput(conf, graph, tmpDir, null);
    } finally {
      FileUtils.delete(tmpDir);
    }
  }

  /**
   * Attempts to run the vertex internally in the current JVM, reading and
   * writing to an in-memory graph. Will start its own zookeeper
   * instance.
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf GiraphClasses specifying which types to use
   * @param graph input graph
   * @param tmpDir file path for storing temporary files.
   * @param checkpointsDir if set, will use this folder
   *                       for storing checkpoints.
   * @return Output graph
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
      V extends Writable,
      E extends Writable> TestGraph<I, V, E> runWithInMemoryOutput(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph,
      File tmpDir,
      String checkpointsDir) throws Exception {
    conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
    InMemoryVertexOutputFormat.initializeOutputGraph(conf);
    InternalVertexRunner.run(conf, graph, tmpDir, checkpointsDir);
    return InMemoryVertexOutputFormat.getOutputGraph();
  }

  /**
   * Configuration options for running local ZK.
   *
   * @param zkDir directory for ZK to hold files in.
   * @return zookeeper configuration object
   */
  private static ZookeeperConfig configLocalZooKeeper(File zkDir) {
    ZookeeperConfig config = new ZookeeperConfig();
    config.setMaxSessionTimeout(100000);
    config.setMinSessionTimeout(10000);
    config.setClientPortAddress(new InetSocketAddress("localhost", 0));
    config.setDataDir(zkDir.getAbsolutePath());
    return config;
  }

}
