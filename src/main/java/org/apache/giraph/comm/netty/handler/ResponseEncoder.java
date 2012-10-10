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

import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * How a server should respond to a client. Currently only used for
 * responding to client's SASL messages, and removed after client
 * authenticates.
 */
public class ResponseEncoder extends OneToOneEncoder {
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(ResponseEncoder.class);
  /** Holds the place of the message length until known. */
  private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

  @Override
  protected Object encode(ChannelHandlerContext ctx,
      Channel channel, Object msg) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("encode(" + ctx + "," + channel + "," + msg);
    }

    if (!(msg instanceof WritableRequest)) {
      throw new IllegalArgumentException(
          "encode: cannot encode message of type " + msg.getClass() +
              " since it is not an instance of an implementation of " +
              " WritableRequest.");
    }
    @SuppressWarnings("unchecked")
    WritableRequest writableRequest =
      (WritableRequest) msg;
    ChannelBufferOutputStream outputStream =
      new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(
        10, ctx.getChannel().getConfig().getBufferFactory()));

    if (LOG.isDebugEnabled()) {
      LOG.debug("encode: Encoding a message of type " + msg.getClass());
    }
    outputStream.write(LENGTH_PLACEHOLDER);

    // write type of object.
    outputStream.writeByte(writableRequest.getType().ordinal());

    // write the object itself.
    writableRequest.write(outputStream);

    outputStream.flush();

    // Set the correct size at the end.
    ChannelBuffer encodedBuffer = outputStream.buffer();
    encodedBuffer.setInt(0, encodedBuffer.writerIndex() - 4);

    if (LOG.isDebugEnabled()) {
      LOG.debug("encode: Encoding a message of type " + msg.getClass());
    }
/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
    if (writableRequest.getType() == RequestType.SASL_COMPLETE_REQUEST) {
      // We are sending to the client a SASL_COMPLETE response (created by
      // the SaslServer handler). The SaslServer handler has removed itself
      // from the pipeline after creating this response, and now it's time for
      // the ResponseEncoder to remove itself also.
      if (LOG.isDebugEnabled()) {
        LOG.debug("encode: Removing RequestEncoder handler: no longer needed," +
            " since client: " + ctx.getChannel().getRemoteAddress() + " has " +
            "completed authenticating.");
      }
      ctx.getPipeline().remove(this);
    }
/*end[HADOOP_NON_SECURE]*/
    return encodedBuffer;
  }
}

