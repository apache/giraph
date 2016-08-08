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

import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory observer to help synchronize when full gcs are happening across all
 * the workers
 */
public class MemoryObserver {
  /** Whether or not to use memory observer */
  public static final BooleanConfOption USE_MEMORY_OBSERVER =
      new BooleanConfOption("giraph.memoryObserver.enabled", false,
          "Whether or not to use memory observer");
  /** For which fraction of free memory will we issue manual gc calls */
  public static final FloatConfOption FREE_MEMORY_FRACTION_FOR_GC =
      new FloatConfOption("giraph.memoryObserver.freeMemoryFractionForGc", 0.1f,
          "For which fraction of free memory will we issue manual gc calls");
  /** Minimum milliseconds between two manual gc calls */
  public static final IntConfOption MIN_MS_BETWEEN_FULL_GCS =
      new IntConfOption("giraph.memoryObserver.minMsBetweenFullGcs", 60 * 1000,
          "Minimum milliseconds between two manual gc calls");

  /** Logger */
  private static final Logger LOG = Logger.getLogger(MemoryObserver.class);
  /** How long does memory observer thread sleep for */
  private static final int MEMORY_OBSERVER_SLEEP_MS = 1000;

  /** When was the last manual gc call */
  private final AtomicLong lastManualGc = new AtomicLong();
  /** Zookeeper */
  private final ZooKeeperExt zk;
  /** Path on zookeeper for memory observer files */
  private final String zkPath;
  /** Value of conf setting MIN_MS_BETWEEN_FULL_GCS */
  private final int minMsBetweenFullGcs;

  /**
   * Constructor
   *
   * @param zk Zookeeper
   * @param zkPath Path on zookeeper for memory observer files
   * @param conf Configration
   */
  public MemoryObserver(final ZooKeeperExt zk,
      final String zkPath, GiraphConfiguration conf) {
    this.zk = zk;
    this.zkPath = zkPath;
    minMsBetweenFullGcs = MIN_MS_BETWEEN_FULL_GCS.get(conf);

    if (!USE_MEMORY_OBSERVER.get(conf)) {
      return;
    }

    try {
      // Create base path for memory observer nodes
      zk.createOnceExt(zkPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT, true);
    } catch (KeeperException | InterruptedException e) {
      LOG.info("Exception occurred", e);
    }
    setWatcher();

    final float freeMemoryFractionForGc =
        FREE_MEMORY_FRACTION_FOR_GC.get(conf);
    ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {

        while (true) {
          double freeMemoryFraction = MemoryUtils.freeMemoryFraction();
          long msFromLastGc = System.currentTimeMillis() - lastManualGc.get();
          if (msFromLastGc > minMsBetweenFullGcs &&
              freeMemoryFraction < freeMemoryFractionForGc) {
            try {
              if (LOG.isInfoEnabled()) {
                LOG.info("Notifying others about low memory (" +
                    freeMemoryFraction + "% free)");
              }
              zk.createExt(
                  zkPath + "/" + System.currentTimeMillis(),
                  new byte[0],
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  false);
            } catch (KeeperException | InterruptedException e) {
              LOG.warn("Exception occurred", e);
            }
          }
          if (!ThreadUtils.trySleep(MEMORY_OBSERVER_SLEEP_MS)) {
            return;
          }
        }
      }
    }, "memory-observer");
  }

  /** Set watcher on memory observer folder */
  private void setWatcher() {
    try {
      // Set a watcher on this path
      zk.getChildrenExt(zkPath, true, false, false);
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception occurred", e);
    }
  }

  /** Manually call gc, if enough time from last call has passed */
  public void callGc() {
    long last = lastManualGc.get();
    if (System.currentTimeMillis() - last > minMsBetweenFullGcs &&
        lastManualGc.compareAndSet(last, System.currentTimeMillis())) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Calling gc manually");
      }
      System.gc();
      if (LOG.isInfoEnabled()) {
        LOG.info("Manual gc call done");
      }
    }
    setWatcher();
  }
}
