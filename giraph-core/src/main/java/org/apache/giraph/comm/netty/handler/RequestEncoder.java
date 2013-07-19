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
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Requests have a request type and an encoded request.
 */
public class RequestEncoder extends OneToOneEncoder {
  /** Time class to use */
  private static final Time TIME = SystemTime.get();
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(RequestEncoder.class);
  /** Holds the place of the message length until known */
  private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
  /** Buffer starting size */
  private final int bufferStartingSize;
  /** Whether or not to use direct byte buffers */
  private final boolean useDirectBuffers;
  /** Start nanoseconds for the encoding time */
  private long startEncodingNanoseconds = -1;

  /**
   * Constructor.
   *
   * @param conf Giraph configuration
   */
  public RequestEncoder(GiraphConfiguration conf) {
    bufferStartingSize =
        GiraphConstants.NETTY_REQUEST_ENCODER_BUFFER_SIZE.get(conf);
    useDirectBuffers =
        GiraphConstants.NETTY_REQUEST_ENCODER_USE_DIRECT_BUFFERS.get(conf);
  }

  @Override
  protected Object encode(ChannelHandlerContext ctx,
                          Channel channel, Object msg) throws Exception {
    if (!(msg instanceof WritableRequest)) {
      throw new IllegalArgumentException(
          "encode: Got a message of type " + msg.getClass());
    }

    // Encode the request
    if (LOG.isDebugEnabled()) {
      startEncodingNanoseconds = TIME.getNanoseconds();
    }
    WritableRequest writableRequest = (WritableRequest) msg;
    int requestSize = writableRequest.getSerializedSize();
    ChannelBuffer channelBuffer;
    if (requestSize == WritableRequest.UNKNOWN_SIZE) {
      channelBuffer = ChannelBuffers.dynamicBuffer(
          bufferStartingSize,
          ctx.getChannel().getConfig().getBufferFactory());
    } else {
      requestSize += LENGTH_PLACEHOLDER.length + 1;
      channelBuffer = useDirectBuffers ?
          ChannelBuffers.directBuffer(requestSize) :
          ChannelBuffers.buffer(requestSize);
    }
    ChannelBufferOutputStream outputStream =
        new ChannelBufferOutputStream(channelBuffer);
    outputStream.write(LENGTH_PLACEHOLDER);
    outputStream.writeByte(writableRequest.getType().ordinal());
    try {
      writableRequest.write(outputStream);
    } catch (IndexOutOfBoundsException e) {
      LOG.error("encode: Most likely the size of request was not properly " +
          "specified (this buffer is too small) - see getSerializedSize() in " +
          writableRequest.getType().getRequestClass());
      throw new IllegalStateException(e);
    }
    outputStream.flush();
    outputStream.close();

    // Set the correct size at the end
    ChannelBuffer encodedBuffer = outputStream.buffer();
    encodedBuffer.setInt(0, encodedBuffer.writerIndex() - 4);
    if (LOG.isDebugEnabled()) {
      LOG.debug("encode: Client " + writableRequest.getClientId() + ", " +
          "requestId " + writableRequest.getRequestId() +
          ", size = " + encodedBuffer.writerIndex() + ", " +
          writableRequest.getType() + " took " +
          Times.getNanosSince(TIME, startEncodingNanoseconds) + " ns");
    }
    return encodedBuffer;
  }
}
