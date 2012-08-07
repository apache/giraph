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

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Keep track of the bytes sent/received and provide some metrics when
 * desired as part of the Netty Channel stack.
 */
public class ByteCounter extends SimpleChannelHandler {
  /** Megabyte in bytes */
  public static final double MEGABYTE = 1024f * 1024f;
  /** Helper to format the doubles */
  private static final DecimalFormat DOUBLE_FORMAT =
      new DecimalFormat("#######.####");
  /** All bytes ever sent */
  private final AtomicLong bytesSent = new AtomicLong();
  /** Total sent requests */
  private final AtomicLong sentRequests = new AtomicLong();
  /** All bytes ever received */
  private final AtomicLong bytesReceived = new AtomicLong();
  /** Total received requests */
  private final AtomicLong receivedRequests = new AtomicLong();
  /** Start time (for bandwidth calculation) */
  private final AtomicLong startMsecs =
      new AtomicLong(System.currentTimeMillis());

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
    throws Exception {
    if (e instanceof MessageEvent &&
        ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
      ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
      bytesReceived.addAndGet(b.readableBytes());
      receivedRequests.incrementAndGet();
    }

    super.handleUpstream(ctx, e);
  }

  @Override
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
    throws Exception {
    if (e instanceof MessageEvent &&
        ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
      ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
      bytesSent.addAndGet(b.readableBytes());
      sentRequests.incrementAndGet();
    }

    super.handleDownstream(ctx, e);
  }

  /**
   * Reset all the bytes kept track of.
   */
  private void resetBytes() {
    bytesSent.set(0);
    sentRequests.set(0);
    bytesReceived.set(0);
    receivedRequests.set(0);
  }

  /**
   * Reset the start msecs.
   */
  private void resetStartMsecs() {
    startMsecs.set(System.currentTimeMillis());
  }

  /**
   * Reset everything this object keeps track of
   */
  public void resetAll() {
    resetBytes();
    resetStartMsecs();
  }

  public long getBytesSent() {
    return bytesSent.get();
  }

  public long getBytesReceived() {
    return bytesReceived.get();
  }

  /**
   * @return Mbytes sent / sec in the current interval
   */
  public double getMbytesPerSecSent() {
    return bytesSent.get() * 1000f /
        (1 + System.currentTimeMillis() - startMsecs.get()) / MEGABYTE;
  }

  /**
   * @return Mbytes received / sec in the current interval
   */
  public double getMbytesPerSecReceived() {
    return bytesReceived.get() * 1000f /
        (1 + System.currentTimeMillis() - startMsecs.get()) / MEGABYTE;
  }

  /**
   * @return A string containing all the metrics
   */
  public String getMetrics() {
    double mBytesSent = bytesSent.get() / MEGABYTE;
    double mBytesReceived = bytesReceived.get() / MEGABYTE;
    long curSentRequests = sentRequests.get();
    long curReceivedRequests = receivedRequests.get();
    double mBytesSentPerReq =
        (curSentRequests == 0) ? 0 : mBytesSent / curSentRequests;
    double mBytesReceivedPerReq =
        (curReceivedRequests == 0) ? 0 : mBytesReceived / curReceivedRequests;
    return "MBytes/sec sent = " +
        DOUBLE_FORMAT.format(getMbytesPerSecSent()) +
        ", MBytes/sec received = " +
        DOUBLE_FORMAT.format(getMbytesPerSecReceived()) +
        ", MBytesSent = " + DOUBLE_FORMAT.format(mBytesSent) +
        ", MBytesReceived = " + DOUBLE_FORMAT.format(mBytesReceived) +
        ", ave sent request MBytes = " +
        DOUBLE_FORMAT.format(mBytesSentPerReq) +
        ", ave received request MBytes = " +
        DOUBLE_FORMAT.format(mBytesReceivedPerReq) +
        ", secs waited = " +
        ((System.currentTimeMillis() - startMsecs.get()) / 1000f);
  }

  /**
   * Get the metrics if a given window of time has passed.  Return null
   * otherwise.  If the window is met, reset the metrics.
   *
   * @param minMsecsWindow Msecs of the minimum window
   * @return Metrics or else null if the window wasn't met
   */
  public String getMetricsWindow(int minMsecsWindow) {
    if (System.currentTimeMillis() - startMsecs.get() > minMsecsWindow) {
      String metrics = getMetrics();
      resetAll();
      return metrics;
    }

    return null;
  }
}
