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
package org.apache.giraph.graph;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

/**
 * Utility class to extract InputSplit locality information
 * from znodes and to sort the InputSplit list for the worker
 * owning this object to select splits from.
 */
public class LocalityInfoSorter {
  /** The worker's local ZooKeeperExt ref */
  private final ZooKeeperExt zooKeeper;
  /** The List of InputSplit znode paths */
  private final List<String> pathList;
  /** The worker's hostname */
  private final String hostName;

  /**
   * Constructor
   * @param zooKeeper the worker's ZkExt
   * @param pathList the path to read from
   * @param hostName the worker's host name (for matching)
   */
  public LocalityInfoSorter(ZooKeeperExt zooKeeper, List<String> pathList,
    String hostName) {
    this.zooKeeper = zooKeeper;
    this.pathList = pathList;
    this.hostName = hostName;
  }

  /**
   * Re-order list of InputSplits so files local to this worker node's
   * disk are the first it will iterate over when attempting to claim
   * a split to read. This will increase locality of data reads with greater
   * probability as the % of total nodes in the cluster hosting data and workers
   * BOTH increase towards 100%. Replication increases our chances of a "hit."
   *
   * @return the pathList, with host-local splits sorted to the front.
   */
  public List<String> getPrioritizedLocalInputSplits() {
    List<String> sortedList = new ArrayList<String>();
    boolean prioritize;
    String hosts = null;
    for (int index = 0; index < pathList.size(); ++index) {
      final String path = pathList.get(index);
      prioritize = false;
      try {
        hosts = getLocationsFromZkInputSplitData(path);
      } catch (IOException ioe) {
        hosts = null; // no problem, just don't sort this entry
      } catch (KeeperException ke) {
        hosts = null;
      } catch (InterruptedException ie) {
        hosts = null;
      }
      prioritize = hosts == null ? false : hosts.contains(hostName);
      sortedList.add(prioritize ? 0 : index, path);
    }
    return sortedList;
  }

  /**
   * Utility for extracting locality data from an InputSplit ZNode.
   *
   * @param zkSplitPath the input split path to attempt to read
   * ZNode locality data from for this InputSplit.
   * @return an array of String hostnames from ZNode data, or throws
   */
  private String getLocationsFromZkInputSplitData(String zkSplitPath)
    throws IOException, KeeperException, InterruptedException {
    byte[] locationData = zooKeeper.getData(zkSplitPath, false, null);
    DataInputStream inputStream =
      new DataInputStream(new ByteArrayInputStream(locationData));
    // only read the "first" entry in the znode data, the locations
    return Text.readString(inputStream);
  }
}
