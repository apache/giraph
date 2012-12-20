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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.io.IOException;

/**
 * Client-side Netty pipeline component that allows authentication with a
 * server.
 */
public class SaslClientHandler extends OneToOneDecoder {
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
  public void handleUpstream(
    ChannelHandlerContext ctx, ChannelEvent evt)
    throws Exception {
    if (!(evt instanceof MessageEvent)) {
      ctx.sendUpstream(evt);
      return;
    }
    MessageEvent e = (MessageEvent) evt;
    Object originalMessage = e.getMessage();
    Object decodedMessage = decode(ctx, ctx.getChannel(), originalMessage);
    // Generate SASL response to server using Channel-local SASL client.
    SaslNettyClient saslNettyClient = NettyClient.SASL.get(ctx.getChannel());
    if (saslNettyClient == null) {
      throw new Exception("handleUpstream: saslNettyClient was unexpectedly " +
          "null for channel: " + ctx.getChannel());
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
      ctx.getPipeline().remove(this);
      ctx.getPipeline().replace("length-field-based-frame-decoder",
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
    ctx.getChannel().write(saslResponse);
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx,
                          Channel channel, Object msg) throws Exception {
    if (!(msg instanceof ChannelBuffer)) {
      throw new IllegalStateException("decode: Got illegal message " + msg);
    }
    // Decode msg into an object whose class C implements WritableRequest:
    //  C will be either SaslTokenMessage or SaslComplete.
    //
    // 1. Convert message to a stream that can be decoded.
    ChannelBuffer buffer = (ChannelBuffer) msg;
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);
    // 2. Get first byte: message type:
    int enumValue = inputStream.readByte();
    RequestType type = RequestType.values()[enumValue];
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode: Got a response of type " + type + " from server:" +
        channel.getRemoteAddress());
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
    // serverResponse can now be used in the next stage in pipeline.
    return serverResponse;
  }
}
