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
import org.apache.giraph.metrics.MeterDesc;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.NoOpHistogram;
import com.yammer.metrics.core.NoOpMeter;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keep track of the bytes sent/received and provide some metrics when
 * desired as part of the Netty Channel stack.
 */
public class ByteCounter extends SimpleChannelHandler implements
    ResetSuperstepMetricsObserver {
  /** Megabyte in bytes */
  public static final double MEGABYTE = 1024f * 1024f;
  /** Helper to format the doubles */
  private static final DecimalFormat DOUBLE_FORMAT =
      new DecimalFormat("#######.####");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ByteCounter.class);
  /** Class timer */
  private static final Time TIME = SystemTime.get();
  /** All bytes ever sent */
  private final AtomicLong bytesSent = new AtomicLong();
  /** Total sent requests */
  private final AtomicLong sentRequests = new AtomicLong();
  /** All bytes ever received */
  private final AtomicLong bytesReceived = new AtomicLong();
  /** Total received requests */
  private final AtomicLong receivedRequests = new AtomicLong();
  /** Start time (for bandwidth calculation) */
  private final AtomicLong startMsecs = new AtomicLong(TIME.getMilliseconds());
  /** Last updated msecs for getMetricsWindow */
  private final AtomicLong metricsWindowLastUpdatedMsecs = new AtomicLong();

  // Metrics
  /** Meter of requests sent */
  private Meter sentRequestsMeter = NoOpMeter.INSTANCE;
  /** Histogram of bytes sent */
  private Histogram sentBytesHist = NoOpHistogram.INSTANCE;
  /** Meter of requests received */
  private Meter receivedRequestsMeter = NoOpMeter.INSTANCE;
  /** Histogram of bytes received */
  private Histogram receivedBytesHist = NoOpHistogram.INSTANCE;

  /** Constructor */
  public ByteCounter() {
    // Initialize Metrics
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    sentRequestsMeter = superstepMetrics.getMeter(MeterDesc.SENT_REQUESTS);
    sentBytesHist = superstepMetrics.getUniformHistogram(
        MetricNames.SENT_BYTES);
    receivedRequestsMeter = superstepMetrics.getMeter(
        MeterDesc.RECEIVED_REQUESTS);
    receivedBytesHist = superstepMetrics.getUniformHistogram(
        MetricNames.RECEIVED_BYTES);
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
    throws Exception {
    if (e instanceof MessageEvent &&
        ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
      ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
      int receivedBytes = b.readableBytes();
      bytesReceived.addAndGet(receivedBytes);
      receivedBytesHist.update(receivedBytes);
      receivedRequests.incrementAndGet();
      receivedRequestsMeter.mark();
      if (LOG.isDebugEnabled()) {
        LOG.debug("handleUpstream: " + ctx.getName() + " buffer size = " +
            receivedBytes + ", total bytes = " + bytesReceived.get());
      }
    }

    super.handleUpstream(ctx, e);
  }

  @Override
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
    throws Exception {
    if (e instanceof MessageEvent &&
        ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
      ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
      int sentBytes = b.readableBytes();
      bytesSent.addAndGet(sentBytes);
      sentBytesHist.update(sentBytes);
      sentRequests.incrementAndGet();
      sentRequestsMeter.mark();
      if (LOG.isDebugEnabled()) {
        LOG.debug("handleDownstream: " + ctx.getName() + " buffer size = " +
                  sentBytes + ", total bytes = " + bytesSent.get());
      }
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
    startMsecs.set(TIME.getMilliseconds());
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
        (1 + TIME.getMilliseconds() - startMsecs.get()) / MEGABYTE;
  }

  /**
   * @return Mbytes received / sec in the current interval
   */
  public double getMbytesPerSecReceived() {
    return bytesReceived.get() * 1000f /
        (1 + TIME.getMilliseconds() - startMsecs.get()) / MEGABYTE;
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
        ", ave sent req MBytes = " +
        DOUBLE_FORMAT.format(mBytesSentPerReq) +
        ", ave received req MBytes = " +
        DOUBLE_FORMAT.format(mBytesReceivedPerReq) +
        ", secs waited = " +
        ((TIME.getMilliseconds() - startMsecs.get()) / 1000f);
  }

  /**
   * Get the metrics if a given window of time has passed.  Return null
   * otherwise.  If the window is met, reset the metrics.
   *
   * @param minMsecsWindow Msecs of the minimum window
   * @return Metrics or else null if the window wasn't met
   */
  public String getMetricsWindow(int minMsecsWindow) {
    long lastUpdatedMsecs =  metricsWindowLastUpdatedMsecs.get();
    long curMsecs = TIME.getMilliseconds();
    if (curMsecs - lastUpdatedMsecs > minMsecsWindow) {
      // Make sure that only one thread does this update
      if (metricsWindowLastUpdatedMsecs.compareAndSet(lastUpdatedMsecs,
          curMsecs)) {
        String metrics = getMetrics();
        resetAll();
        return metrics;
      }
    }

    return null;
  }
}
