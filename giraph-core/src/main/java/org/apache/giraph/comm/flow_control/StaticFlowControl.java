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

package org.apache.giraph.comm.flow_control;

import com.yammer.metrics.core.Counter;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.giraph.conf.GiraphConstants.WAITING_REQUEST_MSECS;

/**
 * Representation of a flow control that limits the aggregate number of open
 * requests to all other workers to a constant user-defined value
 */
public class StaticFlowControl implements
    FlowControl, ResetSuperstepMetricsObserver {
  /** Maximum number of requests without confirmation we should have */
  public static final IntConfOption MAX_NUMBER_OF_OPEN_REQUESTS =
      new IntConfOption("giraph.maxNumberOfOpenRequests", 10000,
          "Maximum number of requests without confirmation we should have");
  /**
   * After pausing a thread due to too large number of open requests,
   * which fraction of these requests need to be closed before we continue
   */
  public static final FloatConfOption
      FRACTION_OF_REQUESTS_TO_CLOSE_BEFORE_PROCEEDING =
      new FloatConfOption("giraph.fractionOfRequestsToCloseBeforeProceeding",
          0.2f, "Fraction of requests to close before proceeding");
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(StaticFlowControl.class);

  /** Maximum number of requests without confirmation we can have */
  private final int maxNumberOfOpenRequests;
  /**
   * Maximum number of requests that can be open after the pause in order to
   * proceed
   */
  private final int numberOfRequestsToProceed;
  /** Netty client used for sending requests */
  private final NettyClient nettyClient;
  /** Waiting interval for checking outstanding requests msecs */
  private final int waitingRequestMsecs;
  /** Dummy object to wait on until enough open requests get completed */
  private final Object requestSpotAvailable = new Object();
  /** Counter for time spent waiting on too many open requests */
  private Counter timeWaitingOnOpenRequests;
  /** Number of threads waiting on too many open requests */
  private final AtomicInteger numWaitingThreads = new AtomicInteger(0);

  /**
   * Constructor
   *
   * @param conf configuration
   * @param nettyClient netty client
   */
  public StaticFlowControl(ImmutableClassesGiraphConfiguration conf,
                           NettyClient nettyClient) {
    this.nettyClient = nettyClient;
    maxNumberOfOpenRequests = MAX_NUMBER_OF_OPEN_REQUESTS.get(conf);
    numberOfRequestsToProceed = (int) (maxNumberOfOpenRequests *
        (1 - FRACTION_OF_REQUESTS_TO_CLOSE_BEFORE_PROCEEDING.get(conf)));
    if (LOG.isInfoEnabled()) {
      LOG.info("StaticFlowControl: Limit number of open requests to " +
          maxNumberOfOpenRequests + " and proceed when <= " +
          numberOfRequestsToProceed);
    }
    waitingRequestMsecs = WAITING_REQUEST_MSECS.get(conf);
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry metrics) {
    timeWaitingOnOpenRequests = metrics.getCounter(
        MetricNames.TIME_SPENT_WAITING_ON_TOO_MANY_OPEN_REQUESTS_MS);
  }

  @Override
  public void sendRequest(int destTaskId, WritableRequest request) {
    nettyClient.doSend(destTaskId, request);
    if (nettyClient.getNumberOfOpenRequests() > maxNumberOfOpenRequests) {
      long startTime = System.currentTimeMillis();
      waitSomeRequests();
      timeWaitingOnOpenRequests.inc(System.currentTimeMillis() - startTime);
    }
  }

  /**
   * Ensure that at most numberOfRequestsToProceed are not complete.
   * Periodically, check the state of every request.  If we find the connection
   * failed, re-establish it and re-send the request.
   */
  private void waitSomeRequests() {
    numWaitingThreads.getAndIncrement();
    while (nettyClient.getNumberOfOpenRequests() > numberOfRequestsToProceed) {
      // Wait for requests to complete for some time
      synchronized (requestSpotAvailable) {
        if (nettyClient.getNumberOfOpenRequests() <=
            numberOfRequestsToProceed) {
          break;
        }
        try {
          requestSpotAvailable.wait(waitingRequestMsecs);
        } catch (InterruptedException e) {
          throw new IllegalStateException("waitSomeRequests: Got unexpected " +
              "InterruptedException", e);
        }
      }
      nettyClient.logAndSanityCheck();
    }
    numWaitingThreads.getAndDecrement();
  }

  @Override
  public void messageAckReceived(int taskId, long requestId, int response) {
    synchronized (requestSpotAvailable) {
      requestSpotAvailable.notifyAll();
    }
  }

  @Override
  public AckSignalFlag getAckSignalFlag(int response) {
    return AckSignalFlag.values()[response];
  }

  @Override
  public int calculateResponse(AckSignalFlag alreadyDone, int clientId) {
    return alreadyDone.ordinal();
  }

  @Override
  public void logInfo() {
    if (LOG.isInfoEnabled()) {
      LOG.info("logInfo: " + numWaitingThreads.get() + " threads waiting " +
          "until number of open requests falls below " +
          numberOfRequestsToProceed);
    }
  }

  @Override
  public void waitAllRequests() {
    // This flow control policy does not keep any unsent request. All the open
    // requests are in possession of the network client, so the network client
    // will wait for them to complete. Thus, this flow control policy does not
    // need to do anything regarding flushing the remaining requests.
  }

  @Override
  public int getNumberOfUnsentRequests() {
    return 0;
  }
}
