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

import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED;

/**
 * Generic handler of requests.
 *
 * @param <R> Request type
 */
public abstract class RequestServerHandler<R> extends
    SimpleChannelUpstreamHandler {
  /** Number of bytes in the encoded response */
  public static final int RESPONSE_BYTES = 13;
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

  /**
   * Constructor
   *
   * @param workerRequestReservedMap Worker request reservation map
   * @param conf Configuration
   * @param myTaskInfo Current task info
   */
  public RequestServerHandler(
      WorkerRequestReservedMap workerRequestReservedMap,
      ImmutableClassesGiraphConfiguration conf,
      TaskInfo myTaskInfo) {
    this.workerRequestReservedMap = workerRequestReservedMap;
    closeFirstRequest = NETTY_SIMULATE_FIRST_REQUEST_CLOSED.get(conf);
    this.myTaskInfo = myTaskInfo;
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("messageReceived: Got " + e.getMessage().getClass());
    }

    WritableRequest writableRequest = (WritableRequest) e.getMessage();

    // Simulate a closed connection on the first request (if desired)
    if (closeFirstRequest && !ALREADY_CLOSED_FIRST_REQUEST) {
      LOG.info("messageReceived: Simulating closing channel on first " +
          "request " + writableRequest.getRequestId() + " from " +
          writableRequest.getClientId());
      setAlreadyClosedFirstRequest();
      ctx.getChannel().close();
      return;
    }

    // Only execute this request exactly once
    int alreadyDone = 1;
    if (workerRequestReservedMap.reserveRequest(
        writableRequest.getClientId(),
        writableRequest.getRequestId())) {
      if (LOG.isDebugEnabled()) {
        startProcessingNanoseconds = TIME.getNanoseconds();
      }
      processRequest((R) writableRequest);
      if (LOG.isDebugEnabled()) {
        LOG.debug("messageReceived: Processing client " +
            writableRequest.getClientId() + ", " +
            "requestId " + writableRequest.getRequestId() +
            ", " +  writableRequest.getType() + " took " +
            Times.getNanosSince(TIME, startProcessingNanoseconds) + " ns");
      }
      alreadyDone = 0;
    } else {
      LOG.info("messageReceived: Request id " +
          writableRequest.getRequestId() + " from client " +
          writableRequest.getClientId() +
          " was already processed, " +
          "not processing again.");
    }

    // Send the response with the request id
    ChannelBuffer buffer = ChannelBuffers.directBuffer(RESPONSE_BYTES);
    buffer.writeInt(myTaskInfo.getTaskId());
    buffer.writeLong(writableRequest.getRequestId());
    buffer.writeByte(alreadyDone);
    e.getChannel().write(buffer);
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
  public void channelConnected(ChannelHandlerContext ctx,
                               ChannelStateEvent e) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelConnected: Connected the channel on " +
          ctx.getChannel().getRemoteAddress());
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx,
                            ChannelStateEvent e) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelClosed: Closed the channel on " +
          ctx.getChannel().getRemoteAddress() + " with event " +
          e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    LOG.warn("exceptionCaught: Channel failed with " +
        "remote address " + ctx.getChannel().getRemoteAddress(), e.getCause());
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
     * @return New {@link RequestServerHandler}
     */
    RequestServerHandler newHandler(
        WorkerRequestReservedMap workerRequestReservedMap,
        ImmutableClassesGiraphConfiguration conf,
        TaskInfo myTaskInfo);
  }
}
