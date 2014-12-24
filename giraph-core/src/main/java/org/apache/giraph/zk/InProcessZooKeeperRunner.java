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
import org.apache.log4j.Logger;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import javax.management.JMException;
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
  public void start(String zkDir, final String configFilePath) {
    Thread zkThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          quorumRunner.start(configFilePath);
        } catch (IOException e) {
          LOG.error("Unable to start zookeeper", e);
        } catch (QuorumPeerConfig.ConfigException e) {
          LOG.error("Invalid config, zookeeper failed", e);
        }
      }
    });
    zkThread.setDaemon(true);
    zkThread.start();
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
     * @param configFilePath quorum and zookeeper configuration
     * @throws IOException
     * @throws QuorumPeerConfig.ConfigException if config
     * is not formatted properly
     */
    public void start(String configFilePath) throws IOException,
        QuorumPeerConfig.ConfigException {
      QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
      quorumPeerConfig.parse(configFilePath);
      // Start and schedule the the purge task
      DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
          quorumPeerConfig
          .getDataDir(), quorumPeerConfig.getDataLogDir(), quorumPeerConfig
          .getSnapRetainCount(), quorumPeerConfig.getPurgeInterval());
      purgeMgr.start();

      if (quorumPeerConfig.getServers().size() > 0) {
        runFromConfig(quorumPeerConfig);
      } else {
        serverRunner = new ZooKeeperServerRunner();
        serverRunner.start(configFilePath);
      }

      LOG.info("Initialization ended");
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
  private static class ZooKeeperServerRunner extends ZooKeeperServerMain {

    /**
     * Start zookeeper service.
     * @param configFilePath zookeeper configuration file
     * @throws QuorumPeerConfig.ConfigException if config file is not
     * formatted properly
     * @throws IOException
     */
    public void start(String configFilePath) throws
        QuorumPeerConfig.ConfigException, IOException {
      LOG.warn("Either no config or no quorum defined in config, running " +
          " in standalone mode");
      try {
        ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
        LOG.warn("Unable to register log4j JMX control", e);
      }

      ServerConfig serverConfig = new ServerConfig();
      serverConfig.parse(configFilePath);
      runFromConfig(serverConfig);
    }

    /**
     * Stop zookeeper service.
     */
    public void stop() {
      shutdown();
    }
  }
}
