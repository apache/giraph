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

import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.SaslNettyServer;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * Authorize or deny client requests based on existence and completeness
 * of client's SASL authentication.
 */
public class AuthorizeServerHandler extends
    SimpleChannelUpstreamHandler {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(AuthorizeServerHandler.class);

  /**
   * Constructor.
   */
  public AuthorizeServerHandler() {
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("messageReceived: Got " + e.getMessage().getClass());
    }
    // Authorize: client is allowed to doRequest() if and only if the client
    // has successfully authenticated with this server.
    SaslNettyServer saslNettyServer =
        NettyServer.CHANNEL_SASL_NETTY_SERVERS.get(ctx.getChannel());
    if (saslNettyServer == null) {
      LOG.warn("messageReceived: This client is *NOT* authorized to perform " +
          "this action since there's no saslNettyServer to " +
          "authenticate the client: " +
          "refusing to perform requested action: " + e.getMessage());
      return;
    }

    if (!saslNettyServer.isComplete()) {
      LOG.warn("messageReceived: This client is *NOT* authorized to perform " +
          "this action because SASL authentication did not complete: " +
          "refusing to perform requested action: " + e.getMessage());
      // Return now *WITHOUT* sending upstream here, since client
      // not authorized.
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("messageReceived: authenticated client: " +
          saslNettyServer.getUserName() + " is authorized to do request " +
          "on server.");
    }
    // We call sendUpstream() since the client is allowed to perform this
    // request. The client's request will now proceed to the next
    // pipeline component, namely, RequestServerHandler.
    ctx.sendUpstream(e);
  }
}
