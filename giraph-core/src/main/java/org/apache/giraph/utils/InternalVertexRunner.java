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

import org.apache.giraph.conf.GiraphClasses;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Map;
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

  /** Don't construct */
  private InternalVertexRunner() { }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from and
   * writing to a temporary folder on local disk. Will start its own zookeeper
   * instance.
   *
   * @param classes GiraphClasses specifying which types to use
   * @param params a map of parameters to add to the hadoop configuration
   * @param vertexInputData linewise vertex input data
   * @return linewise output data
   * @throws Exception if anything goes wrong
   */
  public static Iterable<String> run(
      GiraphClasses classes,
      Map<String, String> params,
      String[] vertexInputData) throws Exception {
    return run(classes, params, vertexInputData, null);
  }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from and
   * writing to a temporary folder on local disk. Will start its own zookeeper
   * instance.
   *
   * @param classes GiraphClasses specifying which types to use
   * @param params a map of parameters to add to the hadoop configuration
   * @param vertexInputData linewise vertex input data
   * @param edgeInputData linewise edge input data
   * @return linewise output data
   * @throws Exception if anything goes wrong
   */
  public static Iterable<String> run(
      GiraphClasses classes,
      Map<String, String> params,
      String[] vertexInputData,
      String[] edgeInputData) throws Exception {
    File tmpDir = null;
    try {
      // Prepare input file, output folder and temporary folders
      tmpDir = FileUtils.createTestDir(classes.getVertexClass());

      File vertexInputFile = null;
      File edgeInputFile = null;
      if (classes.hasVertexInputFormat()) {
        vertexInputFile = FileUtils.createTempFile(tmpDir, "vertices.txt");
      }
      if (classes.hasEdgeInputFormat()) {
        edgeInputFile = FileUtils.createTempFile(tmpDir, "edges.txt");
      }

      File outputDir = FileUtils.createTempDir(tmpDir, "output");
      File zkDir = FileUtils.createTempDir(tmpDir, "_bspZooKeeper");
      File zkMgrDir = FileUtils.createTempDir(tmpDir, "_defaultZkManagerDir");
      File checkpointsDir = FileUtils.createTempDir(tmpDir, "_checkpoints");

      // Write input data to disk
      if (classes.hasVertexInputFormat()) {
        FileUtils.writeLines(vertexInputFile, vertexInputData);
      }
      if (classes.hasEdgeInputFormat()) {
        FileUtils.writeLines(edgeInputFile, edgeInputData);
      }

      // Create and configure the job to run the vertex
      GiraphJob job = new GiraphJob(classes.getVertexClass().getName());
      GiraphConfiguration conf = job.getConfiguration();
      conf.setVertexClass(classes.getVertexClass());
      conf.setVertexEdgesClass(classes.getVertexEdgesClass());
      conf.setVertexValueFactoryClass(classes.getVertexValueFactoryClass());
      if (classes.hasVertexInputFormat()) {
        conf.setVertexInputFormatClass(classes.getVertexInputFormatClass());
      }
      if (classes.hasEdgeInputFormat()) {
        conf.setEdgeInputFormatClass(classes.getEdgeInputFormatClass());
      }
      if (classes.hasVertexOutputFormat()) {
        conf.setVertexOutputFormatClass(classes.getVertexOutputFormatClass());
      }
      if (classes.hasWorkerContextClass()) {
        conf.setWorkerContextClass(classes.getWorkerContextClass());
      }
      if (classes.hasPartitionContextClass()) {
        conf.setPartitionContextClass(classes.getPartitionContextClass());
      }
      if (classes.hasCombinerClass()) {
        conf.setVertexCombinerClass(classes.getCombinerClass());
      }
      if (classes.hasMasterComputeClass()) {
        conf.setMasterComputeClass(classes.getMasterComputeClass());
      }

      conf.setWorkerConfiguration(1, 1, 100.0f);
      conf.setBoolean(GiraphConstants.SPLIT_MASTER_WORKER, false);
      conf.setBoolean(GiraphConstants.LOCAL_TEST_MODE, true);
      conf.set(GiraphConstants.ZOOKEEPER_LIST, "localhost:" +
          String.valueOf(LOCAL_ZOOKEEPER_PORT));

      conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
      conf.set(GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY,
          zkMgrDir.toString());
      conf.set(GiraphConstants.CHECKPOINT_DIRECTORY, checkpointsDir.toString());

      for (Map.Entry<String, String> param : params.entrySet()) {
        conf.set(param.getKey(), param.getValue());
      }

      Job internalJob = job.getInternalJob();
      if (classes.hasVertexInputFormat()) {
        GiraphFileInputFormat.addVertexInputPath(internalJob.getConfiguration(),
            new Path(vertexInputFile.toString()));
      }
      if (classes.hasEdgeInputFormat()) {
        GiraphFileInputFormat.addEdgeInputPath(internalJob.getConfiguration(),
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
        job.run(true);
      } finally {
        executorService.shutdown();
        zookeeper.end();
      }

      if (classes.hasVertexOutputFormat()) {
        return Files.readLines(new File(outputDir, "part-m-00000"),
            Charsets.UTF_8);
      } else {
        return ImmutableList.of();
      }
    } finally {
      FileUtils.delete(tmpDir);
    }
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
      shutdown();
    }
  }
}
