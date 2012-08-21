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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

/**
 * Decodes encoded requests from the client.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class RequestDecoder<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends OneToOneDecoder {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RequestDecoder.class);
  /** Configuration */
  private final Configuration conf;
  /** Registry of requests */
  private final RequestRegistry requestRegistry;
  /** Byte counter to output */
  private final ByteCounter byteCounter;

  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param requestRegistry Request registry
   * @param byteCounter Keeps track of the decoded bytes
   */
  public RequestDecoder(
      Configuration conf, RequestRegistry requestRegistry,
      ByteCounter byteCounter) {
    this.conf = conf;
    this.requestRegistry = requestRegistry;
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

    ChannelBuffer buffer = (ChannelBuffer) msg;
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);
    int enumValue = inputStream.readByte();
    RequestRegistry.Type type = RequestRegistry.Type.values()[enumValue];
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode: Got a request of type " + type);
    }
    @SuppressWarnings("unchecked")
    Class<? extends WritableRequest<I, V, E, M>> writableRequestClass =
        (Class<? extends WritableRequest<I, V, E, M>>)
        requestRegistry.getClass(type);
    WritableRequest<I, V, E, M> writableRequest =
        ReflectionUtils.newInstance(writableRequestClass, conf);
    writableRequest.readFields(inputStream);
    return writableRequest;
  }
}
