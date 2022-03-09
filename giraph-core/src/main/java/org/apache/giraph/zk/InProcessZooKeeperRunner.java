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
package org.apache.giraph.zk;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import javax.management.JMException;
import java.io.File;
import java.io.IOException;

/**
 * Zookeeper wrapper that starts zookeeper withing master process.
 */
public class InProcessZooKeeperRunner
    extends DefaultImmutableClassesGiraphConfigurable
    implements ZooKeeperRunner {

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InProcessZooKeeperRunner.class);
  /**
   * Wrapper for zookeeper quorum.
   */
  private QuorumRunner quorumRunner = new QuorumRunner();

  @Override
  public int start(String zkDir, ZookeeperConfig config) throws IOException {
    return quorumRunner.start(config);
  }

  @Override
  public void stop() {
    try {
      quorumRunner.stop();
    } catch (InterruptedException e) {
      LOG.error("Unable to cleanly shutdown zookeeper", e);
    }
  }

  @Override
  public void cleanup() {
  }

  /**
   * Wrapper around zookeeper quorum. Does not necessarily
   * starts quorum, if there is only one server in config file
   * will only start zookeeper.
   */
  private static class QuorumRunner extends QuorumPeerMain {

    /**
     * ZooKeeper server wrapper.
     */
    private ZooKeeperServerRunner serverRunner;

    /**
     * Starts quorum and/or zookeeper service.
     * @param config quorum and zookeeper configuration
     * @return zookeeper port
     * @throws IOException if can't start zookeeper
     */
    public int start(ZookeeperConfig config) throws IOException {
      serverRunner = new ZooKeeperServerRunner();
      //Make sure zookeeper starts first and purge manager last
      //This is important because zookeeper creates a folder
      //strucutre on the local disk. Purge manager also tries
      //to create it but from a different thread and can run into
      //race condition. See FileTxnSnapLog source code for details.
      int port = serverRunner.start(config);
      // Start and schedule the the purge task
      DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
          config
              .getDataDir(), config.getDataLogDir(),
          GiraphConstants.ZOOKEEPER_SNAP_RETAIN_COUNT,
          GiraphConstants.ZOOKEEPER_PURGE_INTERVAL);
      purgeMgr.start();

      return port;
    }

    /**
     * Stop quorum and/or zookeeper.
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException {
      if (quorumPeer != null) {
        quorumPeer.shutdown();
        quorumPeer.join();
      } else if (serverRunner != null) {
        serverRunner.stop();
      } else {
        LOG.warn("Neither quorum nor server is set");
      }
    }
  }

  /**
   * Wrapper around zookeeper service.
   */
  public static class ZooKeeperServerRunner  {
    /**
     * Reference to zookeeper factory.
     */
    private ServerCnxnFactory cnxnFactory;
    /**
     * Reference to zookeeper server.
     */
    private ZooKeeperServer zkServer;

    /**
     * Start zookeeper service.
     * @param config zookeeper configuration
     * formatted properly
     * @return the port zookeeper has started on.
     * @throws IOException
     */
    public int start(ZookeeperConfig config) throws IOException {
      LOG.warn("Either no config or no quorum defined in config, " +
          "running in process");
      try {
        ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
        LOG.warn("Unable to register log4j JMX control", e);
      }

      runFromConfig(config);
      ThreadUtils.startThread(new Runnable() {
        @Override
        public void run() {
          try {
            cnxnFactory.join();
            if (zkServer.isRunning()) {
              zkServer.shutdown();
            }
          } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
          }

        }
      }, "zk-thread");
      return zkServer.getClientPort();
    }


    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     */
    public void runFromConfig(ZookeeperConfig config) throws IOException {
      LOG.info("Starting server");
      try {
        // Note that this thread isn't going to be doing anything else,
        // so rather than spawning another thread, we will just call
        // run() in this thread.
        // create a file logger url from the command line args
        zkServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(new
            File(config.getDataLogDir()), new File(config.getDataDir()));
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(GiraphConstants.DEFAULT_ZOOKEEPER_TICK_TIME);
        zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
        zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
        cnxnFactory = ServerCnxnFactory.createFactory();
        cnxnFactory.configure(config.getClientPortAddress(),
            GiraphConstants.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS);
        cnxnFactory.startup(zkServer);
      } catch (InterruptedException e) {
        // warn, but generally this is ok
        LOG.warn("Server interrupted", e);
      }
    }


    /**
     * Stop zookeeper service.
     */
    public void stop() {
      cnxnFactory.shutdown();
    }
  }
}
