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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of JobProgressTrackerService
 */
public class DefaultJobProgressTrackerService
    implements JobProgressTrackerService {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(JobProgressTrackerService.class);
  /** How often to print job's progress */
  private static final int UPDATE_MILLISECONDS = 10 * 1000;

  /** Configuration */
  private GiraphConfiguration conf;
  /** Giraph job callback */
  private GiraphJobObserver jobObserver;
  /** Thread which periodically writes job's progress */
  private Thread writerThread;
  /** Whether application is finished */
  private volatile boolean finished = false;
  /** Number of mappers which the job got */
  private int mappersStarted;
  /** Last time number of mappers started was logged */
  private long lastTimeMappersStartedLogged;
  /** Map of worker progresses */
  private final Map<Integer, WorkerProgress> workerProgresses =
      new ConcurrentHashMap<>();
  /** Job */
  private Job job;

  @Override
  public void init(GiraphConfiguration conf, GiraphJobObserver jobObserver) {
    this.conf = conf;
    this.jobObserver = jobObserver;

    if (LOG.isInfoEnabled()) {
      LOG.info("Waiting for job to start... (this may take a minute)");
    }
    startWriterThread();
  }

  /**
   * Start the thread which writes progress periodically
   */
  private void startWriterThread() {
    writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!finished) {
          if (mappersStarted == conf.getMaxWorkers() + 1 &&
              !workerProgresses.isEmpty()) {
            // Combine and log
            CombinedWorkerProgress combinedWorkerProgress =
                new CombinedWorkerProgress(workerProgresses.values(), conf);
            if (LOG.isInfoEnabled()) {
              LOG.info(combinedWorkerProgress.toString());
            }
            // Check if application is done
            if (combinedWorkerProgress.isDone(conf.getMaxWorkers())) {
              break;
            }
          }
          try {
            Thread.sleep(UPDATE_MILLISECONDS);
          } catch (InterruptedException e) {
            if (LOG.isInfoEnabled()) {
              LOG.info("Progress thread interrupted");
            }
            break;
          }
        }
      }
    });
    writerThread.setDaemon(true);
    writerThread.start();
  }

  @Override
  public void setJob(Job job) {
    this.job = job;
  }

  /**
   * Called when job got all mappers, used to check MAX_ALLOWED_JOB_TIME_MS
   * and potentially start a thread which will kill the job after this time
   */
  private void jobGotAllMappers() {
    jobObserver.jobGotAllMappers(job);
    final long maxAllowedJobTimeMs =
        GiraphConstants.MAX_ALLOWED_JOB_TIME_MS.get(conf);
    if (maxAllowedJobTimeMs > 0) {
      // Start a thread which will kill the job if running for too long
      Thread killThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(maxAllowedJobTimeMs);
            try {
              LOG.warn("Killing job because it took longer than " +
                  maxAllowedJobTimeMs + " milliseconds");
              job.killJob();
            } catch (IOException e) {
              LOG.warn("Failed to kill job", e);
            }
          } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Thread checking for jobs max allowed time " +
                  "interrupted");
            }
          }
        }
      });
      killThread.setDaemon(true);
      killThread.start();
    }
  }

  @Override
  public synchronized void mapperStarted() {
    mappersStarted++;
    if (LOG.isInfoEnabled()) {
      if (mappersStarted == conf.getMaxWorkers() + 1) {
        LOG.info("Got all " + mappersStarted + " mappers");
        jobGotAllMappers();
      } else {
        if (System.currentTimeMillis() - lastTimeMappersStartedLogged >
            UPDATE_MILLISECONDS) {
          lastTimeMappersStartedLogged = System.currentTimeMillis();
          LOG.info("Got " + mappersStarted + " but needs " +
              (conf.getMaxWorkers() + 1) + " mappers");
        }
      }
    }
  }

  @Override
  public void logInfo(String logLine) {
    if (LOG.isInfoEnabled()) {
      LOG.info(logLine);
    }
  }

  @Override
  public void logError(String logLine) {
    LOG.error(logLine);
  }

  @Override
  public void logFailure(String reason) {
    LOG.fatal(reason);
    finished = true;
    writerThread.interrupt();
  }

  @Override
  public void updateProgress(WorkerProgress workerProgress) {
    workerProgresses.put(workerProgress.getTaskId(), workerProgress);
  }

  @Override
  public void stop(boolean succeeded) {
    finished = true;
    writerThread.interrupt();
    if (LOG.isInfoEnabled()) {
      LOG.info("Job " + (succeeded ? "finished successfully" : "failed") +
          ", cleaning up...");
    }
  }

  /**
   * Create job progress server on job client if enabled in configuration.
   *
   * @param conf        Configuration
   * @param jobObserver Giraph job callbacks
   * @return JobProgressTrackerService
   */
  public static JobProgressTrackerService createJobProgressTrackerService(
      GiraphConfiguration conf, GiraphJobObserver jobObserver) {
    if (!conf.trackJobProgressOnClient()) {
      return null;
    }

    JobProgressTrackerService jobProgressTrackerService =
        GiraphConstants.JOB_PROGRESS_TRACKER_CLASS.newInstance(conf);
    jobProgressTrackerService.init(conf, jobObserver);
    return jobProgressTrackerService;
  }
}
