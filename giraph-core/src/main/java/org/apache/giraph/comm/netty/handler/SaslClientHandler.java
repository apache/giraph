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
import org.apache.giraph.comm.netty.SaslNettyClient;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.SaslCompleteRequest;
import org.apache.giraph.comm.requests.SaslTokenMessageRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;

/**
 * Client-side Netty pipeline component that allows authentication with a
 * server.
 */
public class SaslClientHandler extends ChannelInboundHandlerAdapter {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(SaslClientHandler.class);
  /** Configuration */
  private final Configuration conf;

  /**
   * Constructor.
   *
   * @param conf Configuration
   */
  public SaslClientHandler(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
    WritableRequest decodedMessage = decode(ctx, msg);
    // Generate SASL response to server using Channel-local SASL client.
    SaslNettyClient saslNettyClient = ctx.attr(NettyClient.SASL).get();
    if (saslNettyClient == null) {
      throw new Exception("handleUpstream: saslNettyClient was unexpectedly " +
          "null for channel: " + ctx.channel());
    }
    if (decodedMessage.getClass() == SaslCompleteRequest.class) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("handleUpstream: Server has sent us the SaslComplete " +
            "message. Allowing normal work to proceed.");
      }
      synchronized (saslNettyClient.getAuthenticated()) {
        saslNettyClient.getAuthenticated().notify();
      }
      if (!saslNettyClient.isComplete()) {
        LOG.error("handleUpstream: Server returned a Sasl-complete message, " +
            "but as far as we can tell, we are not authenticated yet.");
        throw new Exception("handleUpstream: Server returned a " +
            "Sasl-complete message, but as far as " +
            "we can tell, we are not authenticated yet.");
      }
      // Remove SaslClientHandler and replace LengthFieldBasedFrameDecoder
      // from client pipeline.
      ctx.pipeline().remove(this);
      ctx.pipeline().replace("length-field-based-frame-decoder",
          "fixed-length-frame-decoder",
          new FixedLengthFrameDecoder(RequestServerHandler.RESPONSE_BYTES));
      return;
    }
    SaslTokenMessageRequest serverToken =
      (SaslTokenMessageRequest) decodedMessage;
    if (LOG.isDebugEnabled()) {
      LOG.debug("handleUpstream: Responding to server's token of length: " +
          serverToken.getSaslToken().length);
    }
    // Generate SASL response (but we only actually send the response if it's
    // non-null.
    byte[] responseToServer = saslNettyClient.saslResponse(serverToken);
    if (responseToServer == null) {
      // If we generate a null response, then authentication has completed (if
      // not, warn), and return without sending a response back to the server.
      if (LOG.isDebugEnabled()) {
        LOG.debug("handleUpstream: Response to server is null: " +
            "authentication should now be complete.");
      }
      if (!saslNettyClient.isComplete()) {
        LOG.warn("handleUpstream: Generated a null response, " +
            "but authentication is not complete.");
      }
      return;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("handleUpstream: Response to server token has length:" +
            responseToServer.length);
      }
    }
    // Construct a message containing the SASL response and send it to the
    // server.
    SaslTokenMessageRequest saslResponse =
      new SaslTokenMessageRequest(responseToServer);
    ctx.channel().writeAndFlush(saslResponse);
  }

  /**
   * Decode the message read by handler
   *
   * @param ctx channel handler context
   * @param msg message to decode into a writable request
   * @return decoded writablerequest object
   * @throws Exception
   */
  protected WritableRequest decode(ChannelHandlerContext ctx, Object msg)
    throws Exception {
    if (!(msg instanceof ByteBuf)) {
      throw new IllegalStateException("decode: Got illegal message " + msg);
    }
    // Decode msg into an object whose class C implements WritableRequest:
    //  C will be either SaslTokenMessage or SaslComplete.
    //
    // 1. Convert message to a stream that can be decoded.
    ByteBuf buf = (ByteBuf) msg;
    ByteBufInputStream inputStream = new ByteBufInputStream(buf);
    // 2. Get first byte: message type:
    int enumValue = inputStream.readByte();
    RequestType type = RequestType.values()[enumValue];
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode: Got a response of type " + type + " from server:" +
        ctx.channel().remoteAddress());
    }
    // 3. Create object of the type determined in step 2.
    Class<? extends WritableRequest> writableRequestClass =
      type.getRequestClass();
    WritableRequest serverResponse =
      ReflectionUtils.newInstance(writableRequestClass, conf);
    // 4. Deserialize the inputStream's contents into the newly-constructed
    // serverResponse object.
    try {
      serverResponse.readFields(inputStream);
    } catch (IOException e) {
      LOG.error("decode: Exception when trying to read server response: " + e);
    }
    ReferenceCountUtil.release(buf);
    // serverResponse can now be used in the next stage in pipeline.
    return serverResponse;
  }
}
