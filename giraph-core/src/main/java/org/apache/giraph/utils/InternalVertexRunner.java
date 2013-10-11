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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  /** ZooKeeper port to use for tests */
  public static final int LOCAL_ZOOKEEPER_PORT = 22182;

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
    File tmpDir = null;
    try {
      // Prepare input file, output folder and temporary folders
      tmpDir = FileUtils.createTestDir(conf.getComputationName());

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
      File checkpointsDir = FileUtils.createTempDir(tmpDir, "_checkpoints");

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
      conf.set(GiraphConstants.ZOOKEEPER_LIST, "localhost:" +
          String.valueOf(LOCAL_ZOOKEEPER_PORT));

      conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
      GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
          zkMgrDir.toString());
      GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkpointsDir.toString());

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
      FileOutputFormat.setOutputPath(job.getInternalJob(),
                                     new Path(outputDir.toString()));

      // Configure a local zookeeper instance
      Properties zkProperties = configLocalZooKeeper(zkDir);

      QuorumPeerConfig qpConfig = new QuorumPeerConfig();
      qpConfig.parseProperties(zkProperties);

      // Create and run the zookeeper instance
      final InternalZooKeeper zookeeper = new InternalZooKeeper();
      final ServerConfig zkConfig = new ServerConfig();
      zkConfig.readFrom(qpConfig);

      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            zookeeper.runFromConfig(zkConfig);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      try {
        if (!job.run(true)) {
          return null;
        }
      } finally {
        executorService.shutdown();
        zookeeper.end();
      }

      File outFile = new File(outputDir, "part-m-00000");
      if (conf.hasVertexOutputFormat() && outFile.canRead()) {
        return Files.readLines(outFile, Charsets.UTF_8);
      } else {
        return ImmutableList.of();
      }
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
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
    V extends Writable,
    E extends Writable> void run(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph) throws Exception {
    File tmpDir = null;
    try {
      // Prepare temporary folders
      tmpDir = FileUtils.createTestDir(conf.getComputationName());

      File zkDir = FileUtils.createTempDir(tmpDir, "_bspZooKeeper");
      File zkMgrDir = FileUtils.createTempDir(tmpDir, "_defaultZkManagerDir");
      File checkpointsDir = FileUtils.createTempDir(tmpDir, "_checkpoints");

      conf.setVertexInputFormatClass(InMemoryVertexInputFormat.class);

      // Create and configure the job to run the vertex
      GiraphJob job = new GiraphJob(conf, conf.getComputationName());

      InMemoryVertexInputFormat.setGraph(graph);

      conf.setWorkerConfiguration(1, 1, 100.0f);
      GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
      GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
      conf.set(GiraphConstants.ZOOKEEPER_LIST, "localhost:" +
          String.valueOf(LOCAL_ZOOKEEPER_PORT));

      conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
      GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
          zkMgrDir.toString());
      GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkpointsDir.toString());

      // Configure a local zookeeper instance
      Properties zkProperties = configLocalZooKeeper(zkDir);

      QuorumPeerConfig qpConfig = new QuorumPeerConfig();
      qpConfig.parseProperties(zkProperties);

      // Create and run the zookeeper instance
      final InternalZooKeeper zookeeper = new InternalZooKeeper();
      final ServerConfig zkConfig = new ServerConfig();
      zkConfig.readFrom(qpConfig);

      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            zookeeper.runFromConfig(zkConfig);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      try {
        job.run(true);
      } finally {
        executorService.shutdown();
        zookeeper.end();
      }
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
   * @return Output graph
   * @throws Exception if anything goes wrong
   */
  public static <I extends WritableComparable,
      V extends Writable,
      E extends Writable> TestGraph<I, V, E> runWithInMemoryOutput(
      GiraphConfiguration conf,
      TestGraph<I, V, E> graph) throws Exception {
    conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
    InMemoryVertexOutputFormat.initializeOutputGraph(conf);
    InternalVertexRunner.run(conf, graph);
    return InMemoryVertexOutputFormat.getOutputGraph();
  }

  /**
   * Configuration options for running local ZK.
   *
   * @param zkDir directory for ZK to hold files in.
   * @return Properties configured for local ZK.
   */
  private static Properties configLocalZooKeeper(File zkDir) {
    Properties zkProperties = new Properties();
    zkProperties.setProperty("tickTime", "2000");
    zkProperties.setProperty("dataDir", zkDir.getAbsolutePath());
    zkProperties.setProperty("clientPort",
        String.valueOf(LOCAL_ZOOKEEPER_PORT));
    zkProperties.setProperty("maxClientCnxns", "10000");
    zkProperties.setProperty("minSessionTimeout", "10000");
    zkProperties.setProperty("maxSessionTimeout", "100000");
    zkProperties.setProperty("initLimit", "10");
    zkProperties.setProperty("syncLimit", "5");
    zkProperties.setProperty("snapCount", "50000");
    return zkProperties;
  }

  /**
   * Extension of {@link ZooKeeperServerMain} that allows programmatic shutdown
   */
  private static class InternalZooKeeper extends ZooKeeperServerMain {
    /**
     * Shutdown the ZooKeeper instance.
     */
    void end() {
      if (getCnxnFactory() != null) {
        shutdown();
      }
    }

    /**
     * Get the ZooKeeper connection factory using reflection.
     * @return {@link NIOServerCnxn.Factory} from ZooKeeper
     */
    private NIOServerCnxn.Factory getCnxnFactory() {
      NIOServerCnxn.Factory factory = null;
      try {
        Field field = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        field.setAccessible(true);
        factory = (NIOServerCnxn.Factory) field.get(this);
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Couldn't get cnxn factory", e);
      }
      return factory;
    }
  }
}
