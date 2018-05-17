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


import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.out;
import static org.apache.giraph.conf.GiraphConstants.ZOOKEEPER_SERVER_PORT;

/**
 * A Utility class to be used by Giraph admins to occasionally clean up the
 * ZK remnants of jobs that have failed or were killed before finishing.
 * Usage (note that defaults are used if giraph.XYZ args are missing):
 * <code>
 * bin/giraph-admin -Dgiraph.zkBaseNode=... -Dgiraph.zkList=...
 * -Dgiraph.zkServerPort=... -cleanZk
 * </code>
 *
 * alterantely, the <code>Configuration</code> file will populate these fields
 * as it would in a <code>bin/giraph</code> run.
 *
 * <strong>WARNING:</strong> Obviously, running this while actual Giraph jobs
 * using your cluster are in progress is <strong>not recommended.</strong>
 */
public class GiraphZooKeeperAdmin implements Watcher, Tool {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** The configuration for this admin run */
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Clean the ZooKeeper of all failed and cancelled in-memory
   * job remnants that pile up on the ZK quorum over time.
   * @param args the input command line arguments, if any.
   * @return the System.exit value to return to the console.
   */
  @Override
  public int run(String[] args) {
    final GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
    final int zkPort = ZOOKEEPER_SERVER_PORT.get(giraphConf);
    final String zkBasePath = giraphConf.get(
      GiraphConstants.BASE_ZNODE_KEY, "") + BspService.BASE_DIR;
    final String[] zkServerList;
    String zkServerListStr = giraphConf.getZookeeperList();
    if (zkServerListStr.isEmpty()) {
      throw new IllegalStateException("GiraphZooKeeperAdmin requires a list " +
        "of ZooKeeper servers to clean.");
    }
    zkServerList = zkServerListStr.split(",");

    out.println("[GIRAPH-ZKADMIN] Attempting to clean Zookeeper " +
      "hosts at: " + Arrays.deepToString(zkServerList));
    out.println("[GIRAPH-ZKADMIN] Connecting on port: " + zkPort);
    out.println("[GIRAPH-ZKADMIN] to ZNode root path: " + zkBasePath);
    try {
      ZooKeeperExt zooKeeper = new ZooKeeperExt(
        formatZkServerList(zkServerList, zkPort),
        GiraphConstants.ZOOKEEPER_SESSION_TIMEOUT.getDefaultValue(),
        GiraphConstants.ZOOKEEPER_OPS_MAX_ATTEMPTS.getDefaultValue(),
        GiraphConstants.ZOOKEEPER_SERVERLIST_POLL_MSECS.getDefaultValue(),
        this);
      doZooKeeperCleanup(zooKeeper, zkBasePath);
      return 0;
    } catch (KeeperException e) {
      System.err.println("[ERROR] Failed to do cleanup of " +
        zkBasePath + " due to KeeperException: " + e.getMessage());
    } catch (InterruptedException e) {
      System.err.println("[ERROR] Failed to do cleanup of " +
        zkBasePath + " due to InterruptedException: " + e.getMessage());
    } catch (UnknownHostException e) {
      System.err.println("[ERROR] Failed to do cleanup of " +
        zkBasePath + " due to UnknownHostException: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("[ERROR] Failed to do cleanup of " +
        zkBasePath + " due to IOException: " + e.getMessage());
    }
    return -1;
  }

  /** Implement watcher to receive event at the end of the cleaner run
   * @param event the WatchedEvent returned by ZK after the cleaning job.
   */
  @Override
  public final void process(WatchedEvent event) {
    out.println("[GIRAPH-ZKADMIN] ZK event received: " + event);
  }

  /**
   * Cleans the ZooKeeper quorum of in-memory failed/killed job fragments.
   * @param zooKeeper the connected ZK instance (session) to delete from.
   * @param zkBasePath the base node to begin erasing from.
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void doZooKeeperCleanup(ZooKeeperExt zooKeeper, String zkBasePath)
    throws KeeperException, InterruptedException {
    try {
      zooKeeper.deleteExt(zkBasePath, -1, false);
      out.println("[GIRAPH-ZKADMIN] Deleted: " + zkBasePath);
    } catch (KeeperException.NotEmptyException e) {
      List<String> childList =
        zooKeeper.getChildrenExt(zkBasePath, false, false, false);
      for (String child : childList) {
        String childPath = zkBasePath + "/" + child;
        doZooKeeperCleanup(zooKeeper, childPath);
      }
      zooKeeper.deleteExt(zkBasePath, -1, false);
      out.println("[GIRAPH-ZKADMIN] Deleted: " + zkBasePath);
    }
  }

  /** Forms ZK server list in a format the ZooKeeperExt object
   * requires to connect to the quorum.
   * @param zkServerList the CSV-style list of hostnames of Zk quorum members.
   * @param zkPort the port the quorum is listening on.
   * @return the formatted zkConnectList for use in the ZkExt constructor.
   * @throws UnknownHostException
   */
  private String formatZkServerList(String[] zkServerList, int zkPort)
    throws UnknownHostException {
    StringBuffer zkConnectList = new StringBuffer();
    for (String zkServer : zkServerList) {
      if (!zkServer.equals("")) {
        zkConnectList.append(zkServer + ":" + zkPort + ",");
      }
    }
    return zkConnectList.substring(0, zkConnectList.length() - 1);
  }

  /** Entry point from shell script
   * @param args the command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GiraphZooKeeperAdmin(), args));
  }
}
