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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED;

/**
 * Generic handler of requests.
 *
 * @param <R> Request type
 */
public abstract class RequestServerHandler<R> extends
  ChannelInboundHandlerAdapter {
  /** Number of bytes in the encoded response */
  public static final int RESPONSE_BYTES = 14;
  /** Time class to use */
  private static Time TIME = SystemTime.get();
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RequestServerHandler.class);
  /** Already closed first request? */
  private static volatile boolean ALREADY_CLOSED_FIRST_REQUEST = false;
  /** Close connection on first request (used for simulating failure) */
  private final boolean closeFirstRequest;
  /** Request reserved map (for exactly one semantics) */
  private final WorkerRequestReservedMap workerRequestReservedMap;
  /** My task info */
  private final TaskInfo myTaskInfo;
  /** Start nanoseconds for the processing time */
  private long startProcessingNanoseconds = -1;
  /** Handler for uncaught exceptions */
  private final Thread.UncaughtExceptionHandler exceptionHandler;
  /** Do we have a limit on the number of open requests per worker */
  private final boolean limitOpenRequestsPerWorker;

  /**
   * Constructor
   *
   * @param workerRequestReservedMap Worker request reservation map
   * @param conf Configuration
   * @param myTaskInfo Current task info
   * @param exceptionHandler Handles uncaught exceptions
   */
  public RequestServerHandler(
      WorkerRequestReservedMap workerRequestReservedMap,
      ImmutableClassesGiraphConfiguration conf,
      TaskInfo myTaskInfo,
      Thread.UncaughtExceptionHandler exceptionHandler) {
    this.workerRequestReservedMap = workerRequestReservedMap;
    closeFirstRequest = NETTY_SIMULATE_FIRST_REQUEST_CLOSED.get(conf);
    this.myTaskInfo = myTaskInfo;
    this.exceptionHandler = exceptionHandler;
    this.limitOpenRequestsPerWorker =
        NettyClient.LIMIT_OPEN_REQUESTS_PER_WORKER.get(conf);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
    if (LOG.isTraceEnabled()) {
      LOG.trace("messageReceived: Got " + msg.getClass());
    }

    WritableRequest request = (WritableRequest) msg;

    // Simulate a closed connection on the first request (if desired)
    if (closeFirstRequest && !ALREADY_CLOSED_FIRST_REQUEST) {
      LOG.info("messageReceived: Simulating closing channel on first " +
          "request " + request.getRequestId() + " from " +
          request.getClientId());
      setAlreadyClosedFirstRequest();
      ctx.close();
      return;
    }

    // Only execute this request exactly once
    AckSignalFlag alreadyDone = AckSignalFlag.DUPLICATE_REQUEST;
    if (workerRequestReservedMap.reserveRequest(
        request.getClientId(),
        request.getRequestId())) {
      if (LOG.isDebugEnabled()) {
        startProcessingNanoseconds = TIME.getNanoseconds();
      }
      processRequest((R) request);
      if (LOG.isDebugEnabled()) {
        LOG.debug("messageReceived: Processing client " +
            request.getClientId() + ", " +
            "requestId " + request.getRequestId() +
            ", " +  request.getType() + " took " +
            Times.getNanosSince(TIME, startProcessingNanoseconds) + " ns");
      }
      alreadyDone = AckSignalFlag.NEW_REQUEST;
    } else {
      LOG.info("messageReceived: Request id " +
          request.getRequestId() + " from client " +
          request.getClientId() +
          " was already processed, " +
          "not processing again.");
    }

    // Send the response with the request id
    ByteBuf buffer = ctx.alloc().buffer(RESPONSE_BYTES);
    buffer.writeInt(myTaskInfo.getTaskId());
    buffer.writeLong(request.getRequestId());
    short signal;
    if (limitOpenRequestsPerWorker) {
      signal = NettyClient.calculateResponse(alreadyDone,
          shouldIgnoreCredit(request.getClientId()), getCurrentMaxCredit());
    } else {
      signal = (short) alreadyDone.ordinal();
    }
    buffer.writeShort(signal);

    ctx.write(buffer);
  }

  /**
   * Get the maximum number of open requests per worker (credit) at the moment
   * the method is called. This number should generally depend on the available
   * memory and processing rate.
   *
   * @return maximum number of open requests for each worker
   */
  protected abstract short getCurrentMaxCredit();

  /**
   * Whether we should ignore credit-based control flow in communicating with
   * task with a given id. Generally, communication with master node does not
   * require any control-flow mechanism.
   *
   * @param taskId id of the task on the other end of the communication
   * @return 0 if credit should be ignored, 1 otherwise
   */
  protected abstract boolean shouldIgnoreCredit(int taskId);

  /**
   * Set the flag indicating already closed first request
   */
  private static void setAlreadyClosedFirstRequest() {
    ALREADY_CLOSED_FIRST_REQUEST = true;
  }

  /**
   * Process request
   *
   * @param request Request to process
   */
  public abstract void processRequest(R request);

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelActive: Connected the channel on " +
          ctx.channel().remoteAddress());
    }
    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelInactive: Closed the channel on " +
          ctx.channel().remoteAddress());
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(
      ChannelHandlerContext ctx, Throwable cause) throws Exception {
    exceptionHandler.uncaughtException(Thread.currentThread(), cause);
  }

  /**
   * Factory for {@link RequestServerHandler}
   */
  public interface Factory {
    /**
     * Create new {@link RequestServerHandler}
     *
     * @param workerRequestReservedMap Worker request reservation map
     * @param conf Configuration to use
     * @param myTaskInfo Current task info
     * @param exceptionHandler Handles uncaught exceptions
     * @return New {@link RequestServerHandler}
     */
    RequestServerHandler newHandler(
        WorkerRequestReservedMap workerRequestReservedMap,
        ImmutableClassesGiraphConfiguration conf,
        TaskInfo myTaskInfo,
        Thread.UncaughtExceptionHandler exceptionHandler);
  }
}
