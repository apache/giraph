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

import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED;

/**
 * Generic handler of requests.
 *
 * @param <R> Request type
 */
public abstract class RequestServerHandler<R> extends
  ChannelInboundHandlerAdapter {
  /** Number of bytes in the encoded response */
  public static final int RESPONSE_BYTES = 16;
  /** Time class to use */
  private static Time TIME = SystemTime.get();
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RequestServerHandler.class);
  /** Already closed first request? */
  private static volatile boolean ALREADY_CLOSED_FIRST_REQUEST = false;
  /** Flow control used in sending requests */
  protected FlowControl flowControl;
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
  /** Whether it is the first time reading/handling a request*/
  private final AtomicBoolean firstRead = new AtomicBoolean(true);
  /** Cached value for NETTY_AUTO_READ configuration option */
  private final boolean nettyAutoRead;

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
    this.nettyAutoRead = GiraphConstants.NETTY_AUTO_READ.get(conf);
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
    int signal =
        flowControl.calculateResponse(alreadyDone, request.getClientId());
    buffer.writeInt(signal);
    ctx.write(buffer);
    // NettyServer is bootstrapped with auto-read set to true by default. After
    // the first request is processed, we set auto-read to false. This prevents
    // netty from reading requests continuously and putting them in off-heap
    // memory. Instead, we will call `read` on requests one by one, so that the
    // lower level transport layer handles the congestion if the rate of
    // incoming requests is more than the available processing capability.
    if (!nettyAutoRead && firstRead.compareAndSet(true, false)) {
      ctx.channel().config().setAutoRead(false);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if (!nettyAutoRead) {
      ctx.read();
    } else {
      super.channelReadComplete(ctx);
    }
  }

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

    /**
     * Inform the factory about the flow control policy used (this method should
     * be called before any call to `#newHandle()`)
     *
     * @param flowControl reference to flow control used
     */
    void setFlowControl(FlowControl flowControl);
  }
}
