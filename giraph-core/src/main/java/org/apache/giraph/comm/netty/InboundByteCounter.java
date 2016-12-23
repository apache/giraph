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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandler.Sharable;


/**
 * Keep track of the bytes received and provide some metrics when
 * desired as part of the Netty Channel stack.
 */
@Sharable
public class InboundByteCounter extends ChannelInboundHandlerAdapter implements
    ByteCounter, ResetSuperstepMetricsObserver {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InboundByteCounter.class);
  /** ByteCounter delegate object */
  private final ByteCounterDelegate delegate = new ByteCounterDelegate(true);

  /** Constructor */
  public InboundByteCounter() {
    // Initialize Metrics
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  public long getBytesReceived() {
    return delegate.getBytesProcessed();
  }

  /**
   * Returns bytes received per superstep.
   * @return Number of bytes.
   */
  public long getBytesReceivedPerSuperstep() {
    return delegate.getBytesProcessedPerSuperstep();
  }

  /**
   * Set bytes received per superstep to 0.
   */
  public void resetBytesReceivedPerSuperstep() {
    delegate.resetBytesProcessedPerSuperstep();
  }

  /**
   * @return Mbytes received / sec in the current interval
   */
  public double getMbytesPerSecReceived() {
    return delegate.getMbytesPerSecProcessed();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      int receivedBytes = delegate.byteBookkeeper(buf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("channelRead: " + ctx.channel().toString() + " buffer " +
            "size = " + receivedBytes + ", total bytes = " +
            getBytesReceived());
      }
    }
    ctx.fireChannelRead(msg);
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
