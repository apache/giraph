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

package org.apache.giraph.yarn;

import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import org.junit.Test;


/**
 * Tests the Giraph on YARN workflow. Basically, the plan is to use a
 * <code>MiniYARNCluster</code> to run a small test job through our
 * GiraphYarnClient -&gt; GiraphApplicationMaster -gt; GiraphYarnTask (2 no-ops)
 * No "real" BSP code need be tested here, as it is not aware it is running on
 * YARN once the job is in progress, so the existing MRv1 BSP tests are fine.
 */
public class TestYarnJob implements Watcher {
  private static final Logger LOG = Logger.getLogger(TestYarnJob.class);
  /**
   * Simple No-Op vertex to test if we can run a quick Giraph job on YARN.
   */
  private static class DummyYarnComputation extends BasicComputation<
      IntWritable, IntWritable, NullWritable, IntWritable> {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
      vertex.voteToHalt();
    }
  }

  /** job name for this integration test */
  private static final String JOB_NAME = "giraph-TestPureYarnJob";
  /** ZooKeeper port to use for tests, avoiding InternalVertexRunner's port */
  private static final int LOCAL_ZOOKEEPER_PORT = 22183;
  /** ZooKeeper list system property */
  private static final String zkList = "localhost:" + LOCAL_ZOOKEEPER_PORT;
  /** Local ZK working dir, avoid InternalVertexRunner naming */
  private static final String zkDirName = "_bspZooKeeperYarn";
  /** Local ZK Manager working dir, avoid InternalVertexRunner naming */
  private static final String zkMgrDirName = "_defaultZooKeeperManagerYarn";

  /** Temp ZK base working dir for integration test */
  private File testBaseDir = null;
  /** Fake input dir for integration test */
  private File inputDir = null;
  /** Fake output dir for integration test */
  private File outputDir = null;
  /** Temp ZK working dir for integration test */
  private File zkDir = null;
  /** Temp ZK Manager working dir for integration test */
  private File zkMgrDir = null;
  /** Internal ZooKeeper instance for integration test run */
  private InternalZooKeeper zookeeper;
  /** For running the ZK instance locally */
  private ExecutorService exec = Executors.newSingleThreadExecutor();
  /** GiraphConfiguration for a "fake YARN job" */
  private GiraphConfiguration conf = null;
  /** Counter for # of znode events during integration test */
  private int zkEventCount = 0;
  /** Our YARN test cluster for local integration test */
  private MiniYARNCluster cluster = null;

  @Test
  public void testPureYarnJob() {
    try {
      setupYarnConfiguration();
      initLocalZookeeper();
      initYarnCluster();
      GiraphYarnClient testGyc = new GiraphYarnClient(conf, JOB_NAME);
      Assert.assertTrue(testGyc.run(true));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Caught exception in TestYarnJob: " + e);
    } finally {
      zookeeper.end();
      exec.shutdown();
      cluster.stop();
      deleteTempDirectories();
    }
  }

  /**
   * Logging this stuff will help you debug integration test issues.
   * @param zkEvent incoming event for our current test ZK's znode tree.
   */
  @Override
  public void process(WatchedEvent zkEvent) {
    String event = zkEvent == null ? "NULL" : zkEvent.toString();
    LOG.info("TestYarnJob observed ZK event: " + event +
      " for a total of " + (++zkEventCount) + " so far.");
  }

  /**
   * Delete our temp dir so checkstyle and rat plugins are happy.
   */
  private void deleteTempDirectories() {
    try {
      if (testBaseDir != null && testBaseDir.exists()) {
        FileUtils.deleteDirectory(testBaseDir);
      }
    } catch (IOException ioe) {
      LOG.error("TestYarnJob#deleteTempDirectories() FAIL at: " + testBaseDir);
    }
  }

  /**
   * Initialize a local ZK instance for our test run.
   */
  private void initLocalZookeeper() throws IOException {
    zookeeper = new InternalZooKeeper();
    exec.execute(new Runnable() {
      @Override
      public void run() {
        try {
          // Configure a local zookeeper instance
          Properties zkProperties = generateLocalZkProperties();
          QuorumPeerConfig qpConfig = new QuorumPeerConfig();
          qpConfig.parseProperties(zkProperties);
          // run the zookeeper instance
          final ServerConfig zkConfig = new ServerConfig();
          zkConfig.readFrom(qpConfig);
          zookeeper.runFromConfig(zkConfig);
        } catch (QuorumPeerConfig.ConfigException qpcce) {
          throw new RuntimeException("parse of generated ZK config file " +
                                       "has failed.", qpcce);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("initLocalZookeeper in TestYarnJob: ", e);
        }
      }

      /**
       * Returns pre-created ZK conf properties for Giraph integration test.
       * @return the populated properties sheet.
       */
      Properties generateLocalZkProperties() {
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
    });
  }

  /**
   * Set up the GiraphConfiguration settings we need to run a no-op Giraph
   * job on a MiniYARNCluster as an integration test. Some YARN-specific
   * flags are set inside GiraphYarnClient and won't need to be set here.
   */
  private void setupYarnConfiguration() throws IOException {
    conf = new GiraphConfiguration();
    conf.setWorkerConfiguration(1, 1, 100.0f);
    conf.setMaxMasterSuperstepWaitMsecs(30 * 1000);
    conf.setEventWaitMsecs(3 * 1000);
    conf.setYarnLibJars(""); // no need
    conf.setYarnTaskHeapMb(256); // small since no work to be done
    conf.setComputationClass(DummyYarnComputation.class);
    conf.setVertexInputFormatClass(IntIntNullTextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.setNumComputeThreads(1);
    conf.setMaxTaskAttempts(1);
    conf.setNumInputSplitsThreads(1);
    // Giraph on YARN only ever things its running in "non-local" mode
    conf.setLocalTestMode(false);
    // this has to happen here before we populate the conf with the temp dirs
    setupTempDirectories();
    conf.set(OUTDIR, new Path(outputDir.getAbsolutePath()).toString());
    GiraphFileInputFormat.addVertexInputPath(conf, new Path(inputDir.getAbsolutePath()));
    // hand off the ZK info we just created to our no-op job
    GiraphConstants.ZOOKEEPER_SERVERLIST_POLL_MSECS.set(conf, 500);
    conf.setZooKeeperConfiguration(zkList);
    conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.getAbsolutePath());
    GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf, zkMgrDir.getAbsolutePath());
    // without this, our "real" client won't connect w/"fake" YARN cluster
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
  }

  /**
   * Initialize the temp dir tree for ZK and I/O for no-op integration test.
   */
  private void setupTempDirectories() throws IOException {
    try {
    testBaseDir =
      new File(System.getProperty("user.dir"), JOB_NAME);
    if (testBaseDir.exists()) {
      testBaseDir.delete();
    }
    testBaseDir.mkdir();
    inputDir = new File(testBaseDir, "yarninput");
    if (inputDir.exists()) {
      inputDir.delete();
    }
    inputDir.mkdir();
    File inFile = new File(inputDir, "graph_data.txt");
    inFile.createNewFile();
    outputDir = new File(testBaseDir, "yarnoutput");
    if (outputDir.exists()) {
      outputDir.delete();
    } // don't actually produce the output dir, let Giraph On YARN do it
    zkDir = new File(testBaseDir, zkDirName);
    if (zkDir.exists()) {
      zkDir.delete();
    }
    zkDir.mkdir();
    zkMgrDir = new File(testBaseDir, zkMgrDirName);
    if (zkMgrDir.exists()) {
      zkMgrDir.delete();
    }
    zkMgrDir.mkdir();
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new IOException("from setupTempDirectories: ", ioe);
    }
  }

  /**
   * Initialize the MiniYARNCluster for the integration test.
   */
  private void initYarnCluster() {
    cluster = new MiniYARNCluster(TestYarnJob.class.getName(), 1, 1, 1);
    cluster.init(new ImmutableClassesGiraphConfiguration(conf));
    cluster.start();
  }

  /**
   * Extension of {@link ZooKeeperServerMain} that allows programmatic shutdown
   */
  class InternalZooKeeper extends ZooKeeperServerMain {
    /**
     * Shutdown the ZooKeeper instance.
     */
    void end() {
      shutdown();
    }
  }
}
