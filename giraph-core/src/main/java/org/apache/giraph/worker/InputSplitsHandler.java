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

package org.apache.giraph.worker;

import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores the list of input split paths, and provides thread-safe way for
 * reserving input splits.
 */
public class InputSplitsHandler implements Watcher  {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InputSplitsHandler.class);

  /** The List of InputSplit znode paths */
  private final List<String> pathList;
  /** Current position in the path list */
  private final AtomicInteger currentIndex;
  /** The worker's local ZooKeeperExt ref */
  private final ZooKeeperExt zooKeeper;
  /** Context for reporting progress */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** ZooKeeper input split reserved node. */
  private final String inputSplitReservedNode;
  /** ZooKeeper input split finished node. */
  private final String inputSplitFinishedNode;

  /**
   * Constructor
   *
   * @param splitOrganizer Input splits organizer
   * @param zooKeeper The worker's local ZooKeeperExt ref
   * @param context Context for reporting progress
   * @param inputSplitReservedNode ZooKeeper input split reserved node
   * @param inputSplitFinishedNode ZooKeeper input split finished node
   */
  public InputSplitsHandler(InputSplitPathOrganizer splitOrganizer,
      ZooKeeperExt zooKeeper, Mapper<?, ?, ?, ?>.Context context,
      String inputSplitReservedNode, String inputSplitFinishedNode) {
    this.pathList = Lists.newArrayList(splitOrganizer.getPathList());
    this.currentIndex = new AtomicInteger(0);
    this.zooKeeper = zooKeeper;
    this.context = context;
    this.inputSplitReservedNode = inputSplitReservedNode;
    this.inputSplitFinishedNode = inputSplitFinishedNode;
  }


  /**
   * Try to reserve an InputSplit for loading.  While InputSplits exists that
   * are not finished, wait until they are.
   *
   * NOTE: iterations on the InputSplit list only halt for each worker when it
   * has scanned the entire list once and found every split marked RESERVED.
   * When a worker fails, its Ephemeral RESERVED znodes will disappear,
   * allowing other iterating workers to claim it's previously read splits.
   * Only when the last worker left iterating on the list fails can a danger
   * of data loss occur. Since worker failure in INPUT_SUPERSTEP currently
   * causes job failure, this is OK. As the failure model evolves, this
   * behavior might need to change. We could add watches on
   * inputSplitFinishedNodes and stop iterating only when all these nodes
   * have been created.
   *
   * @return reserved InputSplit or null if no unfinished InputSplits exist
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String reserveInputSplit() throws KeeperException,
      InterruptedException {
    String reservedInputSplitPath;
    Stat reservedStat;
    while (true) {
      int splitToTry = currentIndex.getAndIncrement();
      if (splitToTry >= pathList.size()) {
        return null;
      }
      String nextSplitToClaim = pathList.get(splitToTry);
      context.progress();
      String tmpInputSplitReservedPath =
          nextSplitToClaim + inputSplitReservedNode;
      reservedStat =
          zooKeeper.exists(tmpInputSplitReservedPath, this);
      if (reservedStat == null) {
        try {
          // Attempt to reserve this InputSplit
          zooKeeper.createExt(tmpInputSplitReservedPath,
              null,
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL,
              false);
          reservedInputSplitPath = nextSplitToClaim;
          if (LOG.isInfoEnabled()) {
            float percentFinished =
                splitToTry * 100.0f / pathList.size();
            LOG.info("reserveInputSplit: Reserved input " +
                "split path " + reservedInputSplitPath +
                ", overall roughly " +
                +percentFinished +
                "% input splits reserved");
          }
          return reservedInputSplitPath;
        } catch (KeeperException.NodeExistsException e) {
          LOG.info("reserveInputSplit: Couldn't reserve " +
              "(already reserved) inputSplit" +
              " at " + tmpInputSplitReservedPath);
        } catch (KeeperException e) {
          throw new IllegalStateException(
              "reserveInputSplit: KeeperException on reserve", e);
        } catch (InterruptedException e) {
          throw new IllegalStateException(
              "reserveInputSplit: InterruptedException " +
                  "on reserve", e);
        }
      }
    }
  }

  /**
   * Mark an input split path as completed by this worker.  This notifies
   * the master and the other workers that this input split has not only
   * been reserved, but also marked processed.
   *
   * @param inputSplitPath Path to the input split.
   */
  public void markInputSplitPathFinished(String inputSplitPath) {
    String inputSplitFinishedPath =
        inputSplitPath + inputSplitFinishedNode;
    try {
      zooKeeper.createExt(inputSplitFinishedPath,
          null,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("markInputSplitPathFinished: " + inputSplitFinishedPath +
          " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "markInputSplitPathFinished: KeeperException on " +
              inputSplitFinishedPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "markInputSplitPathFinished: InterruptedException on " +
              inputSplitFinishedPath, e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getPath() == null) {
      LOG.warn("process: Problem with zookeeper, got event with path null, " +
          "state " + event.getState() + ", event type " + event.getType());
      return;
    }
    // Check if the reservation for the input split was lost
    // (some worker died)
    if (event.getPath().endsWith(inputSplitReservedNode) &&
        event.getType() == Watcher.Event.EventType.NodeDeleted) {
      synchronized (pathList) {
        String split = event.getPath();
        split = split.substring(0, split.indexOf(inputSplitReservedNode));
        pathList.add(split);
        if (LOG.isInfoEnabled()) {
          LOG.info("process: Input split " + split + " lost reservation");
        }
      }
    }
  }
}
