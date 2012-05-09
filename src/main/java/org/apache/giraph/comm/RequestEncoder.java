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
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Requests have a request type and an encoded request.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class RequestEncoder<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends OneToOneEncoder {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(RequestEncoder.class);
  /** Holds the place of the message length until known */
  private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

  @Override
  protected Object encode(ChannelHandlerContext ctx,
      Channel channel, Object msg) throws Exception {
    if (!(msg instanceof WritableRequest<?, ?, ?, ?>)) {
      throw new IllegalArgumentException(
          "encode: Got a message of type " + msg.getClass());
    }
    @SuppressWarnings("unchecked")
    WritableRequest<I, V, E, M> writableRequest =
        (WritableRequest<I, V, E, M>) msg;
    ChannelBufferOutputStream outputStream =
        new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(
            10, ctx.getChannel().getConfig().getBufferFactory()));

    outputStream.write(LENGTH_PLACEHOLDER);
    outputStream.writeByte(writableRequest.getType().ordinal());
    writableRequest.write(outputStream);
    outputStream.flush();
    outputStream.close();
    if (LOG.isDebugEnabled()) {
      LOG.debug("encode: Encoding a message of type " + msg.getClass());
    }

    // Set the correct size at the end
    ChannelBuffer encodedBuffer = outputStream.buffer();
    encodedBuffer.setInt(0, encodedBuffer.writerIndex() - 4);
    return encodedBuffer;
  }
}
