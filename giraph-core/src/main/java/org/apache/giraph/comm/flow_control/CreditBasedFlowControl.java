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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.WAITING_REQUEST_MSECS;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.requests.SendResumeRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.utils.AdjustableSemaphore;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Representation of credit-based flow control policy. With this policy there
 * can be limited number of open requests from any worker x to any other worker
 * y. This number is called 'credit'. Let's denote this number by C{x-&gt;y}.
 * This implementation assumes that for a particular worker W, all values of
 * C{x-&gt;W} are the same. Let's denote this value by CR_W. CR_W may change
 * due to other reasons (e.g. memory pressure observed in an out-of-core
 * mechanism). However, CR_W is always in range [0, MAX_CR], where MAX_CR
 * is a user-defined constant. Note that MAX_CR should be representable by
 * at most 14 bits.
 *
 * In this implementation, the value of CR_W is announced to other workers along
 * with the ACK response envelope for all ACK response envelope going out of W.
 * Therefore, for non-zero values of CR_W, other workers know the instant value
 * of CR_W, hence they can control the number of open requests they have to W.
 * However, it is possible that W announces 0 as CR_W. In this case, other
 * workers stop opening more requests to W, hence they will not get any new
 * response envelope from W. This means other workers should be notified with
 * a dedicated request to resume sending more requests once CR_W becomes
 * non-zero. In this implementation, once W_CR is announced as 0 to a particular
 * worker U, we keep U in a set, so later on we can send 'resume signal' to U
 * once CR_W becomes non-zero. Sending resume signals are done through a
 * separate thread.
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
  /**
   * Maximum number of open requests each worker can have to this work at each
   * moment (CR_W -define above- for this worker)
   */
  private volatile short maxOpenRequestsPerWorker;
  /** Total number of unsent, cached requests */
  private final AtomicInteger aggregateUnsentRequests = new AtomicInteger(0);
  /**
   * Map of requests permits per worker. Keys in the map are worker ids and
   * values are pairs (X, Y) where:
   *   X: is the semaphore to control the number of open requests for a
   *      particular worker. Basically, the number of available permits on a
   *      semaphore is the credit available for the worker associated with that
   *      semaphore.
   *   Y: is the timestamp of the latest message (resume signal or ACK response)
   *      that changed the number of permits in the semaphore.
   * The idea behind keeping a timestamp is to avoid any issue that may happen
   * due to out-of-order message delivery. For example, consider this scenario:
   * an ACK response is sent to a worker announcing the credit is 0. Later on,
   * a resume signal announcing a non-zero credit is sent to the same worker.
   * Now, if the resume signal receives before the ACK message, the worker
   * would incorrectly assume credit value of 0, and would avoid sending any
   * messages, which may lead to a live-lock.
   *
   * The timestamp value is simply the request id generated by NettyClient.
   * These ids are generated in consecutive order, hence simulating the concept
   * of timestamp. However, the timestamp value should be sent along with
   * any ACK response envelope. The ACK response envelope is already very small
   * (maybe 10-20 bytes). So, the timestamp value should not add much overhead
   * to it. Instead of sending the whole long value request id (8 bytes) as the
   * timestamp, we can simply send the least significant 2 bytes of it. This is
   * a valid timestamp, as the credit value can be 0x3FFF (=16383) at most. This
   * means there will be at most 0x3FFF messages on the fly at each moment,
   * which means that the timestamp value sent by all messages in fly will fall
   * into a range of size 0x3FFF. So, it is enough to only consider timestamp
   * values twice as big as the mentioned range to be able to accurately
   * determine ordering even when values wrap around. This means we only need to
   * consider 15 least significant bits of request ids as timestamp values.
   *
   * The ACK response value contains following information (from least
   * significant to most significant):
   *  - 16 bits timestamp
   *  - 14 bits credit value
   *  - 1 bit specifying whether one end of communication is master and hence
   *    credit based flow control should be ignored
   *  - 1 bit response flag
   */
  private final ConcurrentMap<Integer, Pair<AdjustableSemaphore, Integer>>
      perWorkerOpenRequestMap = Maps.newConcurrentMap();
  /** Map of unsent cached requests per worker */
  private final ConcurrentMap<Integer, Deque<WritableRequest>>
      perWorkerUnsentRequestMap = Maps.newConcurrentMap();
  /**
   * Set of workers that should be notified to resume sending more requests if
   * the credit becomes non-zero
   */
  private final Set<Integer> workersToResume = Sets.newHashSet();
  /**
   * Resume signals are not using any credit, so they should be treated
   * differently than normal requests. Resume signals should be ignored in
   * accounting for credits in credit-based flow control. The following map
   * keeps sets of request ids, for resume signals sent to other workers that
   * are still "open". The set of request ids used for resume signals for a
   * worker is important so we can determine if a received response is for a
   * resume signal or not.
   */
  private final ConcurrentMap<Integer, Set<Long>> resumeRequestsId =
      Maps.newConcurrentMap();
  /**
   * Queue of the cached requests to be sent out. The queue keeps pairs of
   * (destination id, request). The thread-safe blocking queue is used here for
   * the sake of simplicity. The blocking queue should be bounded (with bounds
   * no more than user-defined max number of unsent/cached requests) to avoid
   * excessive memory footprint.
   */
  private final BlockingQueue<Pair<Integer, WritableRequest>> toBeSent;
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
   * @param exceptionHandler Exception handler
   */
  public CreditBasedFlowControl(ImmutableClassesGiraphConfiguration conf,
                                final NettyClient nettyClient,
                                Thread.UncaughtExceptionHandler
                                    exceptionHandler) {
    this.nettyClient = nettyClient;
    maxOpenRequestsPerWorker =
        (short) MAX_NUM_OF_OPEN_REQUESTS_PER_WORKER.get(conf);
    checkState(maxOpenRequestsPerWorker < 0x4000 &&
        maxOpenRequestsPerWorker > 0, "NettyClient: max number of open " +
        "requests should be in range (0, " + 0x4FFF + ")");
    int maxUnsentRequests = MAX_NUM_OF_UNSENT_REQUESTS.get(conf);
    unsentRequestPermit = new Semaphore(maxUnsentRequests);
    this.toBeSent = new ArrayBlockingQueue<Pair<Integer, WritableRequest>>(
        maxUnsentRequests);
    unsentWaitMsecs = UNSENT_CACHE_WAIT_INTERVAL.get(conf);
    waitingRequestMsecs = WAITING_REQUEST_MSECS.get(conf);

    // Thread to handle/send resume signals when necessary
    ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          synchronized (workersToResume) {
            for (Integer workerId : workersToResume) {
              if (maxOpenRequestsPerWorker != 0) {
                sendResumeSignal(workerId);
              } else {
                break;
              }
            }
            try {
              workersToResume.wait();
            } catch (InterruptedException e) {
              throw new IllegalStateException("run: caught exception " +
                  "while waiting for resume-sender thread to be notified!",
                  e);
            }
          }
        }
      }
    }, "resume-sender", exceptionHandler);

    // Thread to handle/send cached requests
    ThreadUtils.startThread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          Pair<Integer, WritableRequest> pair = null;
          try {
            pair = toBeSent.take();
          } catch (InterruptedException e) {
            throw new IllegalStateException("run: failed while waiting to " +
                "take an element from the request queue!", e);
          }
          int taskId = pair.getLeft();
          WritableRequest request = pair.getRight();
          nettyClient.doSend(taskId, request);
          if (aggregateUnsentRequests.decrementAndGet() == 0) {
            synchronized (aggregateUnsentRequests) {
              aggregateUnsentRequests.notifyAll();
            }
          }
        }
      }
    }, "cached-req-sender", exceptionHandler);
  }

  /**
   * Send resume signal request to a given worker
   *
   * @param workerId id of the worker to send the resume signal to
   */
  private void sendResumeSignal(int workerId) {
    if (maxOpenRequestsPerWorker == 0) {
      LOG.warn("sendResumeSignal: method called while the max open requests " +
          "for worker " + workerId + " is still 0");
      return;
    }
    WritableRequest request = new SendResumeRequest(maxOpenRequestsPerWorker);
    Long resumeId = nettyClient.doSend(workerId, request);
    checkState(resumeId != null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("sendResumeSignal: sending signal to worker " + workerId +
          " with credit=" + maxOpenRequestsPerWorker + ", ID=" +
          (resumeId & 0xFFFF));
    }
    resumeRequestsId.get(workerId).add(resumeId);
  }

  @Override
  public void sendRequest(int destTaskId, WritableRequest request) {
    Pair<AdjustableSemaphore, Integer> pair =
        perWorkerOpenRequestMap.get(destTaskId);
    // Check if this is the first time sending a request to a worker. If so, we
    // should the worker id to necessary bookkeeping data structure.
    if (pair == null) {
      pair = new MutablePair<>(
          new AdjustableSemaphore(maxOpenRequestsPerWorker), -1);
      Pair<AdjustableSemaphore, Integer> temp =
          perWorkerOpenRequestMap.putIfAbsent(destTaskId, pair);
      perWorkerUnsentRequestMap.putIfAbsent(
          destTaskId, new ArrayDeque<WritableRequest>());
      resumeRequestsId.putIfAbsent(
          destTaskId, Sets.<Long>newConcurrentHashSet());
      if (temp != null) {
        pair = temp;
      }
    }
    AdjustableSemaphore openRequestPermit = pair.getLeft();
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
  private boolean shouldIgnoreCredit(int response) {
    return ((short) ((response >> (14 + 16)) & 1)) == 1;
  }

  /**
   * Get the credit from a response
   *
   * @param response response received
   * @return credit from the received response
   */
  private short getCredit(int response) {
    return (short) ((response >> 16) & 0x3FFF);
  }

  /**
   * Get the timestamp from a response
   *
   * @param response response received
   * @return timestamp from the received response
   */
  private int getTimestamp(int response) {
    return response & 0xFFFF;
  }

  /**
   * Get the response flag from a response
   *
   * @param response response received
   * @return AckSignalFlag coming with the response
   */
  @Override
  public AckSignalFlag getAckSignalFlag(int response) {
    return AckSignalFlag.values()[(response >> (16 + 14 + 1)) & 1];
  }

  @Override
  public int calculateResponse(AckSignalFlag flag, int taskId) {
    boolean ignoreCredit = nettyClient.masterInvolved(taskId);
    if (!ignoreCredit && maxOpenRequestsPerWorker == 0) {
      synchronized (workersToResume) {
        workersToResume.add(taskId);
      }
    }
    int timestamp = (int) (nettyClient.getNextRequestId(taskId) & 0xFFFF);
    return (flag.ordinal() << (16 + 14 + 1)) |
        ((ignoreCredit ? 1 : 0) << (16 + 14)) |
        (maxOpenRequestsPerWorker << 16) |
        timestamp;
  }

  @Override
  public void logInfo() {
    if (LOG.isInfoEnabled()) {
      // Count how many unsent requests each task has
      Map<Integer, Integer> unsentRequestCounts = Maps.newHashMap();
      for (Map.Entry<Integer, Deque<WritableRequest>> entry :
          perWorkerUnsentRequestMap.entrySet()) {
        unsentRequestCounts.put(entry.getKey(), entry.getValue().size());
      }
      ArrayList<Map.Entry<Integer, Integer>> sorted =
          Lists.newArrayList(unsentRequestCounts.entrySet());
      Collections.sort(sorted, new Comparator<Map.Entry<Integer, Integer>>() {
        @Override
        public int compare(Map.Entry<Integer, Integer> entry1,
                           Map.Entry<Integer, Integer> entry2) {
          int value1 = entry1.getValue();
          int value2 = entry2.getValue();
          return (value1 < value2) ? 1 : ((value1 == value2) ? 0 : -1);
        }
      });
      StringBuilder message = new StringBuilder();
      message.append("logInfo: ").append(aggregateUnsentRequests.get())
          .append(" unsent requests in total. ");
      int itemsToPrint = Math.min(10, sorted.size());
      for (int i = 0; i < itemsToPrint; ++i) {
        message.append(sorted.get(i).getValue())
            .append(" unsent requests for taskId=")
            .append(sorted.get(i).getKey()).append(" (credit=")
            .append(perWorkerOpenRequestMap.get(sorted.get(i).getKey())
                .getKey().getMaxPermits())
            .append("), ");
      }
      LOG.info(message);
    }
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
          throw new IllegalStateException("waitAllRequests: failed while " +
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
  public void messageAckReceived(int taskId, long requestId, int response) {
    boolean ignoreCredit = shouldIgnoreCredit(response);
    short credit = getCredit(response);
    int timestamp = getTimestamp(response);
    MutablePair<AdjustableSemaphore, Integer> pair =
        (MutablePair<AdjustableSemaphore, Integer>)
            perWorkerOpenRequestMap.get(taskId);
    AdjustableSemaphore openRequestPermit = pair.getLeft();
    // Release a permit on open requests if we received ACK of a request other
    // than a Resume request (resume requests are always sent regardless of
    // number of open requests)
    if (!resumeRequestsId.get(taskId).remove(requestId)) {
      openRequestPermit.release();
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("messageAckReceived: ACK of resume received from " + taskId +
          " timestamp=" + timestamp);
    }
    if (!ignoreCredit) {
      synchronized (pair) {
        if (compareTimestamps(timestamp, pair.getRight()) > 0) {
          pair.setRight(timestamp);
          openRequestPermit.setMaxPermits(credit);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("messageAckReceived: received out-of-order messages." +
              "Received timestamp=" + timestamp + " and current timestamp=" +
              pair.getRight());
        }
      }
    }
    // Since we received a response and we changed the credit of the sender
    // client, we may be able to send some more requests to the sender
    // client. So, we try to send as much request as we can to the sender
    // client.
    trySendCachedRequests(taskId);
  }

  /**
   * Try to send as much as cached requests to a given worker
   *
   * @param taskId id of the worker to send cached requests to
   */
  private void trySendCachedRequests(int taskId) {
    Deque<WritableRequest> requestDeque =
        perWorkerUnsentRequestMap.get(taskId);
    AdjustableSemaphore openRequestPermit =
        perWorkerOpenRequestMap.get(taskId).getLeft();
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
      unsentRequestPermit.release();
      // At this point, we have a request, and we reserved a credit for the
      // sender client. So, we put the request in a queue to be sent to the
      // client.
      try {
        toBeSent.put(
            new ImmutablePair<Integer, WritableRequest>(taskId, request));
      } catch (InterruptedException e) {
        throw new IllegalStateException("trySendCachedRequests: failed while" +
            "waiting to put element in send queue!", e);
      }
    }
  }

  /**
   * Update the max credit that is announced to other workers
   *
   * @param newCredit new credit
   */
  public void updateCredit(short newCredit) {
    newCredit = (short) Math.max(0, Math.min(0x3FFF, newCredit));
    // Check whether we should send resume signals to some workers
    if (maxOpenRequestsPerWorker == 0 && newCredit != 0) {
      maxOpenRequestsPerWorker = newCredit;
      synchronized (workersToResume) {
        workersToResume.notifyAll();
      }
    } else {
      maxOpenRequestsPerWorker = newCredit;
    }
  }

  /**
   * Compare two timestamps accounting for wrap around. Note that the timestamp
   * values should be in a range that fits into 14 bits values. This means if
   * the difference of the two given timestamp is large, we are dealing with one
   * value being wrapped around.
   *
   * @param ts1 first timestamp
   * @param ts2 second timestamp
   * @return positive value if first timestamp is later than second timestamp,
   *         negative otherwise
   */
  private int compareTimestamps(int ts1, int ts2) {
    int diff = ts1 - ts2;
    if (Math.abs(diff) < 0x7FFF) {
      return diff;
    } else {
      return -diff;
    }
  }

  /**
   * Process a resume signal came from a given worker
   *
   * @param clientId id of the worker that sent the signal
   * @param credit the credit value sent along with the resume signal
   * @param requestId timestamp (request id) of the resume signal
   */
  public void processResumeSignal(int clientId, short credit, long requestId) {
    int timestamp = (int) (requestId & 0xFFFF);
    if (LOG.isDebugEnabled()) {
      LOG.debug("processResumeSignal: resume signal from " + clientId +
          " with timestamp=" + timestamp);
    }
    MutablePair<AdjustableSemaphore, Integer> pair =
        (MutablePair<AdjustableSemaphore, Integer>)
            perWorkerOpenRequestMap.get(clientId);
    synchronized (pair) {
      if (compareTimestamps(timestamp, pair.getRight()) > 0) {
        pair.setRight(timestamp);
        pair.getLeft().setMaxPermits(credit);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("processResumeSignal: received out-of-order messages. " +
            "Received timestamp=" + timestamp + " and current timestamp=" +
            pair.getRight());
      }
    }
    trySendCachedRequests(clientId);
  }
}
