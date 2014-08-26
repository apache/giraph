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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.JobProgressTracker;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.log4j.Logger;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.service.RuntimeTTransportException;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

/**
 * Wrapper around JobProgressTracker which retires to connect and swallows
 * exceptions so app wouldn't crash if something goes wrong with progress
 * reports.
 */
public class RetryableJobProgressTrackerClient
    implements JobProgressTrackerClient {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RetryableJobProgressTrackerClient.class);
  /** Configuration */
  private final GiraphConfiguration conf;
  /** Thrift client manager to use to connect to job progress tracker */
  private ThriftClientManager clientManager;
  /** Job progress tracker */
  private JobProgressTracker jobProgressTracker;

  /**
   * Constructor
   *
   * @param conf Giraph configuration
   */
  public RetryableJobProgressTrackerClient(GiraphConfiguration conf) throws
      ExecutionException, InterruptedException {
    this.conf = conf;
    resetConnection();
  }

  /**
   * Try to establish new connection to JobProgressTracker
   */
  private void resetConnection() throws ExecutionException,
      InterruptedException {
    clientManager = new ThriftClientManager();
    FramedClientConnector connector =
        new FramedClientConnector(new InetSocketAddress(
            JOB_PROGRESS_SERVICE_HOST.get(conf),
            JOB_PROGRESS_SERVICE_PORT.get(conf)));
    jobProgressTracker =
        clientManager.createClient(connector, JobProgressTracker.class).get();

  }

  @Override
  public synchronized void cleanup() throws IOException {
    Closeables.close(clientManager, true);
    try {
      clientManager.close();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Exception occurred while trying to close JobProgressTracker", e);
      }
    }
  }

  @Override
  public synchronized void mapperStarted() {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.mapperStarted();
      }
    });
  }

  @Override
  public synchronized void logInfo(final String logLine) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.logInfo(logLine);
      }
    });
  }

  @Override
  public synchronized void logFailure(final String reason) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.logFailure(reason);
      }
    });
  }

  @Override
  public synchronized void updateProgress(final WorkerProgress workerProgress) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.updateProgress(workerProgress);
      }
    });
  }

  /**
   * Execute Runnable, if disconnected try to connect again and retry
   *
   * @param runnable Runnable to execute
   */
  private void executeWithRetry(Runnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeTTransportException te) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("RuntimeTTransportException occurred while talking to " +
            "JobProgressTracker server, trying to reconnect", te);
      }
      try {
        try {
          clientManager.close();
          // CHECKSTYLE: stop IllegalCatch
        } catch (Exception e) {
          // CHECKSTYLE: resume IllegalCatch
          if (LOG.isDebugEnabled()) {
            LOG.debug("");
          }
        }
        resetConnection();
        runnable.run();
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        if (LOG.isInfoEnabled()) {
          LOG.info("Exception occurred while talking to " +
              "JobProgressTracker server, giving up", e);
        }
      }
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      if (LOG.isInfoEnabled()) {
        LOG.info("Exception occurred while talking to " +
            "JobProgressTracker server, giving up", e);
      }
    }
  }
}
