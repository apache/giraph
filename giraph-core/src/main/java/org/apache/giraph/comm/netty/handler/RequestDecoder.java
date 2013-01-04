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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.comm.netty.ByteCounter;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

/**
 * Decodes encoded requests from the client.
 */
public class RequestDecoder extends OneToOneDecoder {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RequestDecoder.class);
  /** Time class to use */
  private static final Time TIME = SystemTime.get();
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** Byte counter to output */
  private final ByteCounter byteCounter;
  /** Start nanoseconds for the decoding time */
  private long startDecodingNanoseconds = -1;
  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param byteCounter Keeps track of the decoded bytes
   */
  public RequestDecoder(ImmutableClassesGiraphConfiguration conf,
                        ByteCounter byteCounter) {
    this.conf = conf;
    this.byteCounter = byteCounter;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx,
      Channel channel, Object msg) throws Exception {
    if (!(msg instanceof ChannelBuffer)) {
      throw new IllegalStateException("decode: Got illegal message " + msg);
    }

    // Output metrics every 1/2 minute
    String metrics = byteCounter.getMetricsWindow(30000);
    if (metrics != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("decode: Server window metrics " + metrics);
      }
    }

    if (LOG.isDebugEnabled()) {
      startDecodingNanoseconds = TIME.getNanoseconds();
    }

    // Decode the request
    ChannelBuffer buffer = (ChannelBuffer) msg;
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);
    int enumValue = inputStream.readByte();
    RequestType type = RequestType.values()[enumValue];
    Class<? extends WritableRequest> writableRequestClass =
        type.getRequestClass();

    WritableRequest writableRequest =
        ReflectionUtils.newInstance(writableRequestClass, conf);
    writableRequest.readFields(inputStream);
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode: Client " + writableRequest.getClientId() +
          ", requestId " + writableRequest.getRequestId() +
          ", " +  writableRequest.getType() + ", with size " +
          buffer.array().length + " took " +
          Times.getNanosSince(TIME, startDecodingNanoseconds) + " ns");
    }

    return writableRequest;
  }
}
