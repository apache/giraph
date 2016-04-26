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

import com.google.common.collect.Maps;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.utils.AdjustableSemaphore;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.WAITING_REQUEST_MSECS;

/**
 * Representation of credit-based flow control policy where each worker has a
 * constant user-defined credit. The number of open requests to a particular
 * worker cannot be more than its specified credit.
 */
public class CreditBasedFlowControl implements FlowControl {
  /**
   * Maximum number of requests we can have per worker without confirmation
   * (i.e. open requests)
   */
  public static final IntConfOption MAX_NUM_OF_OPEN_REQUESTS_PER_WORKER =
      new IntConfOption("giraph.maxOpenRequestsPerWorker", 20,
          "Maximum number of requests without confirmation we can have per " +
              "worker");
  /** Aggregate number of in-memory unsent requests */
  public static final IntConfOption MAX_NUM_OF_UNSENT_REQUESTS =
      new IntConfOption("giraph.maxNumberOfUnsentRequests", 2000,
          "Maximum number of unsent requests we can keep in memory");
  /**
   * Time interval to wait on unsent requests cahce until we find a spot in it
   */
  public static final IntConfOption UNSENT_CACHE_WAIT_INTERVAL =
      new IntConfOption("giraph.unsentCacheWaitInterval", 1000,
          "Time interval to wait on unsent requests cache (in milliseconds)");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(CreditBasedFlowControl.class);

  /** Waiting interval on unsent requests cache until it frees up */
  private final int unsentWaitMsecs;
  /** Waiting interval for checking outstanding requests msecs */
  private final int waitingRequestMsecs;
  /** Maximum number of open requests we can have for each worker */
  private final int maxOpenRequestsPerWorker;
  /** Total number of unsent, cached requests */
  private final AtomicInteger aggregateUnsentRequests = new AtomicInteger(0);
  /**
   * Map of requests permits per worker. Key in the map is the worker id and the
   * value is the semaphore to control the number of open requests for the
   * particular worker. Basically, the number of available permits on this
   * semaphore is the credit available for the worker.
   */
  private final ConcurrentMap<Integer, AdjustableSemaphore>
      perWorkerOpenRequestMap = Maps.newConcurrentMap();
  /** Map of unsent cached requests per worker */
  private final ConcurrentMap<Integer, Deque<WritableRequest>>
      perWorkerUnsentRequestMap = Maps.newConcurrentMap();
  /**
   * Semaphore to control number of cached unsent requests. Maximum number of
   * permits of this semaphore should be equal to MAX_NUM_OF_UNSENT_REQUESTS.
   */
  private final Semaphore unsentRequestPermit;
  /** Netty client used for sending requests */
  private final NettyClient nettyClient;

  /**
   * Constructor
   * @param conf configuration
   * @param nettyClient netty client
   */
  public CreditBasedFlowControl(ImmutableClassesGiraphConfiguration conf,
                                NettyClient nettyClient) {
    this.nettyClient = nettyClient;
    maxOpenRequestsPerWorker = MAX_NUM_OF_OPEN_REQUESTS_PER_WORKER.get(conf);
    checkState(maxOpenRequestsPerWorker < 0x4000 &&
        maxOpenRequestsPerWorker > 0, "NettyClient: max number of open " +
        "requests should be in range (0, " + 0x4FFF + ")");
    unsentRequestPermit = new Semaphore(MAX_NUM_OF_UNSENT_REQUESTS.get(conf));
    unsentWaitMsecs = UNSENT_CACHE_WAIT_INTERVAL.get(conf);
    waitingRequestMsecs = WAITING_REQUEST_MSECS.get(conf);
  }

  @Override
  public void sendRequest(int destTaskId, WritableRequest request) {
    AdjustableSemaphore openRequestPermit =
        perWorkerOpenRequestMap.get(destTaskId);
    // Check if this is the first time sending a request to a worker. If so, we
    // should the worker id to necessary bookkeeping data structure.
    if (openRequestPermit == null) {
      openRequestPermit = new AdjustableSemaphore(maxOpenRequestsPerWorker);
      AdjustableSemaphore temp = perWorkerOpenRequestMap.putIfAbsent(destTaskId,
          openRequestPermit);
      perWorkerUnsentRequestMap
          .putIfAbsent(destTaskId, new ArrayDeque<WritableRequest>());
      if (temp != null) {
        openRequestPermit = temp;
      }
    }
    // Try to reserve a spot for the request amongst the open requests of
    // the destination worker.
    boolean shouldSend = openRequestPermit.tryAcquire();
    boolean shouldCache = false;
    while (!shouldSend) {
      // We should not send the request, and should cache the request instead.
      // It may be possible that the unsent message cache is also full, so we
      // should try to acquire a space on the cache, and if there is no extra
      // space in unsent request cache, we should wait until some space
      // become available. However, it is possible that during the time we are
      // waiting on the unsent messages cache, actual buffer for open requests
      // frees up space.
      try {
        shouldCache = unsentRequestPermit.tryAcquire(unsentWaitMsecs,
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException("shouldSend: failed " +
            "while waiting on the unsent request cache to have some more " +
            "room for extra unsent requests!");
      }
      if (shouldCache) {
        break;
      }
      // We may have an open spot in the meantime that we were waiting on the
      // unsent requests.
      shouldSend = openRequestPermit.tryAcquire();
      if (shouldSend) {
        break;
      }
      // The current thread will be at this point only if it could not make
      // space amongst open requests for the destination worker and has been
      // timed-out in trying to acquire a space amongst unsent messages. So,
      // we should report logs, report progress, and check for request
      // failures.
      nettyClient.logAndSanityCheck();
    }
    // Either shouldSend == true or shouldCache == true
    if (shouldCache) {
      Deque<WritableRequest> unsentRequests =
          perWorkerUnsentRequestMap.get(destTaskId);
      // This synchronize block is necessary for the following reason:
      // Once we are at this point, it means there was no room for this
      // request to become an open request, hence we have to put it into
      // unsent cache. Consider the case that since last time we checked if
      // there is any room for an additional open request so far, all open
      // requests are delivered and their acknowledgements are also processed.
      // Now, if we put this request in the unsent cache, it is not being
      // considered to become an open request, as the only one who checks
      // on this matter would be the one who receives an acknowledgment for an
      // open request for the destination worker. So, a lock is necessary
      // to forcefully serialize the execution if this scenario is about to
      // happen.
      synchronized (unsentRequests) {
        shouldSend = openRequestPermit.tryAcquire();
        if (!shouldSend) {
          aggregateUnsentRequests.getAndIncrement();
          unsentRequests.add(request);
          return;
        }
      }
      // We found a spot amongst open requests to send this request. So, this
      // request won't be cached anymore.
      unsentRequestPermit.release();
    }
    nettyClient.doSend(destTaskId, request);
  }

  /**
   * Whether response specifies that credit should be ignored
   *
   * @param response response received
   * @return true iff credit should be ignored, false otherwise
   */
  private boolean shouldIgnoreCredit(short response) {
    return ((short) ((response >> 14) & 1)) == 1;
  }

  /**
   * Get the credit from a response
   *
   * @param response response received
   * @return credit from the received response
   */
  private short getCredit(short response) {
    return (short) (response & 0x3FFF);
  }

  /**
   * Get the response flag from a response
   *
   * @param response response received
   * @return AckSignalFlag coming with the response
   */
  @Override
  public AckSignalFlag getAckSignalFlag(short response) {
    return AckSignalFlag.values()[(response >> 15) & 1];
  }

  @Override
  public short calculateResponse(AckSignalFlag flag, int taskId) {
    boolean ignoreCredit = nettyClient.masterInvolved(taskId);
    return (short) ((flag.ordinal() << 15) |
        ((ignoreCredit ? 1 : 0) << 14) | (maxOpenRequestsPerWorker & 0x3FFF));
  }

  @Override
  public void waitAllRequests() {
    while (true) {
      synchronized (aggregateUnsentRequests) {
        if (aggregateUnsentRequests.get() == 0) {
          break;
        }
        try {
          aggregateUnsentRequests.wait(waitingRequestMsecs);
        } catch (InterruptedException e) {
          throw new IllegalStateException("waitSomeRequests: failed while " +
              "waiting on open/cached requests");
        }
      }
      if (aggregateUnsentRequests.get() == 0) {
        break;
      }
      nettyClient.logAndSanityCheck();
    }
  }

  @Override
  public int getNumberOfUnsentRequests() {
    return aggregateUnsentRequests.get();
  }

  @Override
  public void messageAckReceived(int taskId, short response) {
    boolean shouldIgnoreCredit = shouldIgnoreCredit(response);
    short credit = getCredit(response);
    AdjustableSemaphore openRequestPermit =
        perWorkerOpenRequestMap.get(taskId);
    openRequestPermit.release();
    if (!shouldIgnoreCredit) {
      openRequestPermit.setMaxPermits(credit);
    }
    Deque<WritableRequest> requestDeque =
        perWorkerUnsentRequestMap.get(taskId);
    // Since we received a response and we changed the credit of the sender
    // client, we may be able to send some more requests to the sender
    // client. So, we try to send as much request as we can to the sender
    // client.
    while (true) {
      WritableRequest request;
      synchronized (requestDeque) {
        request = requestDeque.pollFirst();
        if (request == null) {
          break;
        }
        // See whether the sender client has any unused credit
        if (!openRequestPermit.tryAcquire()) {
          requestDeque.offerFirst(request);
          break;
        }
      }
      // At this point, we have a request, and we reserved a credit for the
      // sender client. So, we send the request to the client and update
      // the state.
      nettyClient.doSend(taskId, request);
      if (aggregateUnsentRequests.decrementAndGet() == 0) {
        synchronized (aggregateUnsentRequests) {
          aggregateUnsentRequests.notifyAll();
        }
      }
      unsentRequestPermit.release();
    }
  }
}
