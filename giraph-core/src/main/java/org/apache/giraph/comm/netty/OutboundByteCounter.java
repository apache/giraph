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

package org.apache.giraph.comm.netty;

import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelHandler.Sharable;

/**
 * Keep track of the bytes sent and provide some metrics when
 * desired as part of the Netty Channel stack.
 */
@Sharable
public class OutboundByteCounter extends ChannelOutboundHandlerAdapter
  implements ByteCounter, ResetSuperstepMetricsObserver {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(OutboundByteCounter.class);
  /** ByteCounter delegate object */
  private final ByteCounterDelegate delegate = new ByteCounterDelegate(false);

  /** Constructor */
  public OutboundByteCounter() {
    // Initialize Metrics
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  public long getBytesSent() {
    return delegate.getBytesProcessed();
  }

  /**
   * @return Mbytes sent / sec in the current interval
   */
  public double getMbytesPerSecSent() {
    return delegate.getMbytesPerSecProcessed();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
    ChannelPromise promise) throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      int sentBytes = delegate.byteBookkeeper(buf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("write: " + ctx.channel().toString() +
            " buffer size = " + sentBytes + ", total bytes = " + getBytesSent()
        );
      }
    }
    ctx.writeAndFlush(msg, promise);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    delegate.newSuperstep(superstepMetrics);
  }

  @Override
  public void resetAll() {
    delegate.resetAll();
  }

  @Override
  public String getMetrics() {
    return delegate.getMetrics();
  }

  @Override
  public String getMetricsWindow(int minMsecsWindow) {
    return delegate.getMetricsWindow(minMsecsWindow);
  }
}

