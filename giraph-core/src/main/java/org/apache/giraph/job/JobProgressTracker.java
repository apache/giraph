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

package org.apache.giraph.job;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.CounterUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class which tracks job's progress on client
 */
public class JobProgressTracker implements Watcher {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(JobProgressTracker.class);
  /** How often to print job's progress */
  private static final int UPDATE_MILLISECONDS = 5 * 1000;
  /** Thread which periodically writes job's progress */
  private Thread writerThread;
  /** ZooKeeperExt */
  private ZooKeeperExt zk;
  /** Whether application is finished */
  private volatile boolean finished = false;

  /**
   * Constructor
   *
   * @param submittedJob Job to track
   * @param conf Configuration
   */
  public JobProgressTracker(final Job submittedJob,
      final GiraphConfiguration conf) throws IOException, InterruptedException {
    String zkServer = CounterUtils.waitAndGetCounterNameFromGroup(
        submittedJob, GiraphConstants.ZOOKEEPER_SERVER_PORT_COUNTER_GROUP);
    final String basePath = CounterUtils.waitAndGetCounterNameFromGroup(
        submittedJob, GiraphConstants.ZOOKEEPER_BASE_PATH_COUNTER_GROUP);
    // Connect to ZooKeeper
    if (zkServer != null && basePath != null) {
      zk = new ZooKeeperExt(
        zkServer,
        conf.getZooKeeperSessionTimeout(),
        conf.getZookeeperOpsMaxAttempts(),
        conf.getZookeeperOpsRetryWaitMsecs(),
        this,
        new Progressable() {
          @Override
          public void progress() {
          }
        });
      writerThread = new Thread(new Runnable() {
        @Override
        public void run() {
          String workerProgressBasePath = basePath +
            BspService.WORKER_PROGRESSES;
          try {
            while (!finished) {
              if (zk.exists(workerProgressBasePath, false) != null) {
                // Get locations of all worker progresses
                List<String> workerProgressPaths = zk.getChildrenExt(
                  workerProgressBasePath, false, false, true);
                List<WorkerProgress> workerProgresses =
                  new ArrayList<WorkerProgress>(workerProgressPaths.size());
                // Read all worker progresses
                for (String workerProgressPath : workerProgressPaths) {
                  WorkerProgress workerProgress = new WorkerProgress();
                  byte[] zkData = zk.getData(workerProgressPath, false, null);
                  WritableUtils.readFieldsFromByteArray(zkData, workerProgress);
                  workerProgresses.add(workerProgress);
                }
                // Combine and log
                CombinedWorkerProgress combinedWorkerProgress =
                  new CombinedWorkerProgress(workerProgresses);
                if (LOG.isInfoEnabled()) {
                  LOG.info(combinedWorkerProgress.toString());
                }
                // Check if application is done
                if (combinedWorkerProgress.isDone(conf.getMaxWorkers())) {
                  break;
                }
              }
              Thread.sleep(UPDATE_MILLISECONDS);
            }
            // CHECKSTYLE: stop IllegalCatchCheck
          } catch (Exception e) {
            // CHECKSTYLE: resume IllegalCatchCheck
            if (LOG.isInfoEnabled()) {
              LOG.info("run: Exception occurred", e);
            }
          } finally {
            try {
              // Create a node so master knows we stopped communicating with
              // ZooKeeper and it's safe to cleanup
              zk.createExt(
                basePath + BspService.CLEANED_UP_DIR + "/client",
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                true);
              zk.close();
              // CHECKSTYLE: stop IllegalCatchCheck
            } catch (Exception e) {
              // CHECKSTYLE: resume IllegalCatchCheck
              if (LOG.isInfoEnabled()) {
                LOG.info("run: Exception occurred", e);
              }
            }
          }
        }
      });
      writerThread.start();
    }
  }

  /**
   * Stop the thread which logs application progress
   */
  public void stop() {
    finished = true;
  }

  @Override
  public void process(WatchedEvent event) {
  }
}

