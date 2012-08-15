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

package org.apache.giraph.comm;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * Generic handler of responses.
 */
public class ResponseClientHandler extends SimpleChannelUpstreamHandler {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ResponseClientHandler.class);
  /** Outstanding request map */
  private final ConcurrentMap<Long, RequestInfo> outstandingRequestMap;

  /**
   * Constructor.
   *
   * @param outstandingRequestMap Map of outstanding requests
   */
  public ResponseClientHandler(
      ConcurrentMap<Long, RequestInfo> outstandingRequestMap) {
    this.outstandingRequestMap = outstandingRequestMap;
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent event) {
    if (!(event.getMessage() instanceof ChannelBuffer)) {
      throw new IllegalStateException("messageReceived: Got a " +
          "non-ChannelBuffer message " + event.getMessage());
    }
    ChannelBuffer buffer = (ChannelBuffer) event.getMessage();
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);
    long requestId = -1;
    int response = -1;
    try {
      requestId = inputStream.readLong();
      response = inputStream.readByte();
      inputStream.close();
    } catch (IOException e) {
      throw new IllegalStateException(
          "messageReceived: Got IOException ", e);
    }
    if (response != 0) {
      throw new IllegalStateException(
          "messageReceived: Got illegal response " + response);
    }

    RequestInfo requestInfo = outstandingRequestMap.remove(requestId);
    if (requestInfo == null) {
      throw new IllegalStateException("messageReceived: Impossible to " +
          "have a non-registered requestId " + requestId);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("messageReceived: Processed request id = " + requestId +
            " " + requestInfo + ".  Waiting on " +
            outstandingRequestMap.size() +
            " requests, bytes = " + buffer.capacity());
      }
    }

    // Help NettyClient#waitSomeRequests() to finish faster
    synchronized (outstandingRequestMap) {
      outstandingRequestMap.notifyAll();
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx,
                            ChannelStateEvent e) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelClosed: Closed the channel on " +
          ctx.getChannel().getRemoteAddress());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    throw new IllegalStateException("exceptionCaught: Channel failed with " +
        "remote address " + ctx.getChannel().getRemoteAddress(), e.getCause());
  }
}
