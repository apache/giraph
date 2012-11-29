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

import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

/**
 * Utility class to extract the list of InputSplits from the
 * ZooKeeper tree of "claimable splits" the master created,
 * and to sort the list to favor local data blocks.
 *
 * This class provides an Iterator for the list the worker will
 * claim splits from, making all sorting and data-code locality
 * processing done here invisible to callers. The aim is to cut
 * down on the number of ZK reads workers perform before locating
 * an unclaimed InputSplit.
 */
public class InputSplitPathOrganizer implements Iterable<String> {
  /** The worker's local ZooKeeperExt ref */
  private final ZooKeeperExt zooKeeper;
  /** The List of InputSplit znode paths */
  private final List<String> pathList;
  /** The worker's hostname */
  private final String hostName;
  /** The adjusted base offset by which to iterate on the path list */
  private int baseOffset;

  /**
   * Constructor
   *
   * @param zooKeeper the worker's ZkExt
   * @param zkPathList the path to read from
   * @param hostName the worker's host name (for matching)
   * @param port the port number for this worker
   */
  public InputSplitPathOrganizer(final ZooKeeperExt zooKeeper,
    final String zkPathList, final String hostName, final int port)
    throws KeeperException, InterruptedException {
    this(zooKeeper, zooKeeper.getChildrenExt(zkPathList, false, false, true),
        hostName, port);
  }

  /**
   * Constructor
   *
   * @param zooKeeper the worker's ZkExt
   * @param inputSplitPathList path of input splits to read from
   * @param hostName the worker's host name (for matching)
   * @param port the port number for this worker
   */
  public InputSplitPathOrganizer(
      final ZooKeeperExt zooKeeper, final List<String> inputSplitPathList,
      final String hostName, final int port)
    throws KeeperException, InterruptedException {
    this.zooKeeper = zooKeeper;
    this.pathList = Lists.newArrayList(inputSplitPathList);
    this.hostName = hostName;
    this.baseOffset = 0; // set later after switching out local paths
    prioritizeLocalInputSplits(port);
  }

 /**
  * Re-order list of InputSplits so files local to this worker node's
  * disk are the first it will iterate over when attempting to claim
  * a split to read. This will increase locality of data reads with greater
  * probability as the % of total nodes in the cluster hosting data and workers
  * BOTH increase towards 100%. Replication increases our chances of a "hit."
  *
  * @param port the port number for hashing unique iteration indexes for all
  *             workers, even those sharing the same host node.
  */
  private void prioritizeLocalInputSplits(final int port) {
    List<String> sortedList = new ArrayList<String>();
    String hosts = null;
    for (Iterator<String> iterator = pathList.iterator(); iterator.hasNext();) {
      final String path = iterator.next();
      try {
        hosts = getLocationsFromZkInputSplitData(path);
      } catch (IOException ioe) {
        hosts = null; // no problem, just don't sort this entry
      } catch (KeeperException ke) {
        hosts = null;
      } catch (InterruptedException ie) {
        hosts = null;
      }
      if (hosts != null && hosts.contains(hostName)) {
        sortedList.add(path); // collect the local block
        iterator.remove(); // remove local block from list
      }
    }
    // shuffle the local blocks in case several workers exist on this host
    Collections.shuffle(sortedList);
    // determine the hash-based offset for this worker to iterate from
    // and place the local blocks into the list at that index, if any
    final int temp = hostName.hashCode() + (19 * port);
    if (pathList.size() != 0) {
      baseOffset = Math.abs(temp % pathList.size());
    }
    // re-insert local paths at "adjusted index zero" for caller to iterate on
    pathList.addAll(baseOffset, sortedList);
  }

  /**
   * Utility for extracting locality data from an InputSplit ZNode.
   *
   * @param zkSplitPath the input split path to attempt to read
   * ZNode locality data from for this InputSplit.
   * @return a String of hostnames from ZNode data, or throws
   */
  private String getLocationsFromZkInputSplitData(String zkSplitPath)
    throws IOException, KeeperException, InterruptedException {
    byte[] locationData = zooKeeper.getData(zkSplitPath, false, null);
    DataInputStream inputStream =
      new DataInputStream(new ByteArrayInputStream(locationData));
    // only read the "first" entry in the znode data, the locations
    return Text.readString(inputStream);
  }

  /**
   * Utility accessor for Input Split znode path list size
   *
   * @return the size of <code>this.pathList</code>
   */
  public int getPathListSize() {
    return this.pathList.size();
  }

  /**
   * Iterator for the pathList
   *
   * @return an iterator for our list of input split paths
   */
  public Iterator<String> iterator() {
    return new PathListIterator();
  }

  /**
   * Iterator for path list that handles the locality and hash offsetting.
   */
  public class PathListIterator implements Iterator<String> {
    /** the current iterator index */
    private int currentIndex = 0;

    /**
     *  Do we have more list to iterate upon?
     *
     *  @return true if more path strings are available
     */
    @Override
    public boolean hasNext() {
      return currentIndex < pathList.size();
    }

    /**
     * Return the next pathList element
     *
     * @return the next input split path
     */
    @Override
    public String next() {
      return pathList.get((baseOffset + currentIndex++) % pathList.size());
    }

    /** Just a placeholder; should not do anything! */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not allowed.");
    }
  }
}
