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
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_RESPONSE_FAILED;

/**
 * Generic handler of responses.
 */
public class ResponseClientHandler extends ChannelInboundHandlerAdapter {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ResponseClientHandler.class);
  /** Already dropped first response? (used if dropFirstResponse == true) */
  private static volatile boolean ALREADY_DROPPED_FIRST_RESPONSE = false;
  /** Drop first response (used for simulating failure) */
  private final boolean dropFirstResponse;
  /** Netty client that does the actual I/O and keeps track of open requests */
  private final NettyClient nettyClient;

  /**
   * Constructor.
   * @param nettyClient Client that does the actual I/O
   * @param conf Configuration
   */
  public ResponseClientHandler(NettyClient nettyClient, Configuration conf) {
    this.nettyClient = nettyClient;
    dropFirstResponse = NETTY_SIMULATE_FIRST_RESPONSE_FAILED.get(conf);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
    if (!(msg instanceof ByteBuf)) {
      throw new IllegalStateException("channelRead: Got a " +
          "non-ByteBuf message " + msg);
    }

    ByteBuf buf = (ByteBuf) msg;
    int senderId = -1;
    long requestId = -1;
    int response = -1;
    try {
      senderId = buf.readInt();
      requestId = buf.readLong();
      response = buf.readInt();
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalStateException(
          "channelRead: Got IndexOutOfBoundsException ", e);
    }
    ReferenceCountUtil.release(buf);

    boolean shouldDrop = false;
    // Simulate a failed response on the first response (if desired)
    if (dropFirstResponse && !ALREADY_DROPPED_FIRST_RESPONSE) {
      LOG.info("channelRead: Simulating dropped response " + response +
          " for request " + requestId);
      setAlreadyDroppedFirstResponse();
      shouldDrop = true;
    }

    nettyClient.messageReceived(senderId, requestId, response, shouldDrop);
  }

  /**
   * Set already dropped first response flag
   */
  private static void setAlreadyDroppedFirstResponse() {
    ALREADY_DROPPED_FIRST_RESPONSE = true;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("channelClosed: Closed the channel on " +
          ctx.channel().remoteAddress());
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    throws Exception {
    LOG.warn("exceptionCaught: Channel channelId=" +
        ctx.channel().hashCode() + " failed with remote address " +
        ctx.channel().remoteAddress(), cause);
  }
}

