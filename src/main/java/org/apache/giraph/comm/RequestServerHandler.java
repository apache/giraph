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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * Generic handler of requests.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class RequestServerHandler<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends SimpleChannelUpstreamHandler {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RequestServerHandler.class);
  /** Data that can be accessed for handling requests */
  private final ServerData<I, V, E, M> serverData;

  /**
   * Constructor with external server data
   *
   * @param serverData Data held by the server
   */
  public RequestServerHandler(ServerData<I, V, E, M> serverData) {
    this.serverData = serverData;
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("messageReceived: Got " + e.getMessage().getClass());
    }
    @SuppressWarnings("unchecked")
    WritableRequest<I, V, E, M> writableRequest =
        (WritableRequest<I, V, E, M>) e.getMessage();
    writableRequest.doRequest(serverData);

    // Send the success response
    ChannelBuffer buffer = ChannelBuffers.directBuffer(1);
    buffer.writeByte(0);
    e.getChannel().write(buffer);
  }
}
