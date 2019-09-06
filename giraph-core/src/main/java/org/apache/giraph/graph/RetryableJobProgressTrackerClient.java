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
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.job.ClientThriftServer;
import org.apache.giraph.job.JobProgressTracker;
import org.apache.giraph.master.MasterProgress;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.log4j.Logger;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.RuntimeTTransportException;
import com.facebook.swift.service.ThriftClientEventHandler;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

/**
 * Wrapper around JobProgressTracker which retries to connect and swallows
 * exceptions so app wouldn't crash if something goes wrong with progress
 * reports.
 */
public class RetryableJobProgressTrackerClient
    implements JobProgressTrackerClient {
  /** Conf option for number of retries */
  public static final IntConfOption RETRYABLE_JOB_PROGRESS_CLIENT_NUM_RETRIES =
    new IntConfOption("giraph.job.progress.client.num.retries", 1,
      "Number of times to retry a failed operation");
  /** Conf option for wait time between retries */
  public static final IntConfOption
    RETRYABLE_JOB_PROGRESS_CLIENT_RETRY_WAIT_MS =
    new IntConfOption("giraph.job.progress.client.retries.wait", 1000,
      "Time (msec) to wait between retries");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RetryableJobProgressTrackerClient.class);
  /** Configuration */
  private final GiraphConfiguration conf;
  /** Thrift client manager to use to connect to job progress tracker */
  private ThriftClientManager clientManager;
  /** Job progress tracker */
  private JobProgressTracker jobProgressTracker;
  /** Cached value for number of retries */
  private int numRetries;
  /** Cached value for wait time between retries */
  private int retryWaitMsec;

  /**
   * Constructor
   *
   * @param conf Giraph configuration
   */
  public RetryableJobProgressTrackerClient(GiraphConfiguration conf) throws
      ExecutionException, InterruptedException {
    this.conf = conf;
    numRetries = RETRYABLE_JOB_PROGRESS_CLIENT_NUM_RETRIES.get(conf);
    retryWaitMsec = RETRYABLE_JOB_PROGRESS_CLIENT_RETRY_WAIT_MS.get(conf);
    resetConnection();
  }

  /**
   * Try to establish new connection to JobProgressTracker
   */
  private void resetConnection() throws ExecutionException,
      InterruptedException {
    clientManager = new ThriftClientManager(
        new ThriftCodecManager(new ThriftCodec[0]),
        new NiftyClient(
            new NettyClientConfigBuilder().setWorkerThreadCount(2).build()),
        ImmutableSet.<ThriftClientEventHandler>of());
    FramedClientConnector connector =
        new FramedClientConnector(new InetSocketAddress(
            ClientThriftServer.CLIENT_THRIFT_SERVER_HOST.get(conf),
            ClientThriftServer.CLIENT_THRIFT_SERVER_PORT.get(conf)));
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
    }, numRetries);
  }

  @Override
  public synchronized void logInfo(final String logLine) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.logInfo(logLine);
      }
    }, numRetries);
  }

  @Override
  public synchronized void logError(final String logLine,
                                    final byte [] exByteArray) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.logError(logLine, exByteArray);
      }
    }, numRetries);
  }

  @Override
  public synchronized void logFailure(final String reason) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.logFailure(reason);
      }
    }, numRetries);
  }

  @Override
  public synchronized void updateProgress(final WorkerProgress workerProgress) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.updateProgress(workerProgress);
      }
    }, numRetries);
  }

  @Override
  public void updateMasterProgress(final MasterProgress masterProgress) {
    executeWithRetry(new Runnable() {
      @Override
      public void run() {
        jobProgressTracker.updateMasterProgress(masterProgress);
      }
    }, numRetries);
  }

  /**
   * Execute Runnable, if disconnected try to connect again and retry
   *
   * @param runnable Runnable to execute
   * @param numRetries Number of retries
   */
  private void executeWithRetry(Runnable runnable, int numRetries) {
    try {
      runnable.run();
    } catch (RuntimeTTransportException | RejectedExecutionException te) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(te.getClass() + " occurred while talking to " +
          "JobProgressTracker server, trying to reconnect", te);
      }
      for (int i = 1; i <= numRetries; i++) {
        try {
          ThreadUtils.trySleep(retryWaitMsec);
          retry(runnable);
          break; // If the retry succeeded, we simply break from the loop

        } catch (RuntimeTTransportException | RejectedExecutionException e) {
          // If a RuntimeTTTransportException happened, then we will retry
          if (LOG.isInfoEnabled()) {
            LOG.info("Exception occurred while talking to " +
              "JobProgressTracker server after retry " + i +
              " of " + numRetries, e);
          }
          // CHECKSTYLE: stop IllegalCatch
        } catch (Exception e) {
          // CHECKSTYLE: resume IllegalCatch
          // If any other exception happened (e.g. application-specific),
          // then we stop.
          LOG.info("Exception occurred while talking to " +
            "JobProgressTracker server after retry " + i +
            " of " + numRetries + ", giving up", e);
          break;
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

  /**
   * Executes a single retry by closing the existing {@link #clientManager}
   * connection, re-initializing it, and then executing the passed instance
   * of {@link Runnable}.
   *
   * @param runnable Instance of {@link Runnable} to execute.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void retry(Runnable runnable) throws ExecutionException,
    InterruptedException {
    try {
      clientManager.close();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Exception occurred while trying to close client manager", e);
      }
    }
    resetConnection();
    runnable.run();
  }
}
