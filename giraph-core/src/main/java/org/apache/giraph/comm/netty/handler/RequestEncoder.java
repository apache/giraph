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

import io.netty.buffer.ByteBufOutputStream;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

/**
 * Requests have a request type and an encoded request.
 */
public class RequestEncoder extends ChannelOutboundHandlerAdapter {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(RequestEncoder.class);
  /** Time class to use */
  private static final Time TIME = SystemTime.get();
  /** Buffer starting size */
  private final int bufferStartingSize;
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
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
    ChannelPromise promise) throws Exception {
    if (!(msg instanceof WritableRequest)) {
      throw new IllegalArgumentException(
          "encode: Got a message of type " + msg.getClass());
    }

    // Encode the request
    if (LOG.isDebugEnabled()) {
      startEncodingNanoseconds = TIME.getNanoseconds();
    }

    ByteBuf buf;
    WritableRequest request = (WritableRequest) msg;
    int requestSize = request.getSerializedSize();
    if (requestSize == WritableRequest.UNKNOWN_SIZE) {
      buf = ctx.alloc().buffer(bufferStartingSize);
    } else {
      requestSize +=  SIZE_OF_INT + SIZE_OF_BYTE;
      buf = ctx.alloc().buffer(requestSize);
    }
    ByteBufOutputStream output = new ByteBufOutputStream(buf);

    // This will later be filled with the correct size of serialized request
    output.writeInt(0);
    output.writeByte(request.getType().ordinal());
    try {
      request.write(output);
    } catch (IndexOutOfBoundsException e) {
      LOG.error("write: Most likely the size of request was not properly " +
          "specified (this buffer is too small) - see getSerializedSize() " +
          "in " + request.getType().getRequestClass());
      throw new IllegalStateException(e);
    }
    output.flush();
    output.close();

    // Set the correct size at the end
    buf.setInt(0, buf.writerIndex() - SIZE_OF_INT);
    if (LOG.isDebugEnabled()) {
      LOG.debug("write: Client " + request.getClientId() + ", " +
          "requestId " + request.getRequestId() +
          ", size = " + buf.readableBytes() + ", " +
          request.getType() + " took " +
          Times.getNanosSince(TIME, startEncodingNanoseconds) + " ns");
    }
    ctx.write(buf, promise);
  }
}
