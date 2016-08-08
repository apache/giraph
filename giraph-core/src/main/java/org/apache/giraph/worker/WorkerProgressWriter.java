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

import org.apache.giraph.job.JobProgressTracker;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.log4j.Logger;

/**
 * Class which periodically writes worker's progress to zookeeper
 */
public class WorkerProgressWriter {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WorkerProgressWriter.class);
  /** How often to update worker's progress */
  private static final int WRITE_UPDATE_PERIOD_MILLISECONDS = 10 * 1000;

  /** Job progress tracker */
  private final JobProgressTracker jobProgressTracker;
  /** Thread which writes worker's progress */
  private final Thread writerThread;
  /** Whether worker finished application */
  private volatile boolean finished = false;

  /**
   * Constructor, starts separate thread to periodically update worker's
   * progress
   *
   * @param jobProgressTracker JobProgressTracker to report job progress to
   */
  public WorkerProgressWriter(JobProgressTracker jobProgressTracker) {
    this.jobProgressTracker = jobProgressTracker;
    writerThread = ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (!finished) {
          updateAndSendProgress();
          double factor = 1 + Math.random();
          if (!ThreadUtils.trySleep(
              (long) (WRITE_UPDATE_PERIOD_MILLISECONDS * factor))) {
            break;
          }
        }
      }
    }, "workerProgressThread");
  }

  /**
   * Update worker progress and send it
   */
  private void updateAndSendProgress() {
    WorkerProgress.get().updateMemory();
    jobProgressTracker.updateProgress(WorkerProgress.get());
  }

  /**
   * Stop the thread which writes worker's progress
   */
  public void stop() throws InterruptedException {
    finished = true;
    writerThread.interrupt();
    writerThread.join();
    updateAndSendProgress();
  }
}
