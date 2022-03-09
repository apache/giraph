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

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.NoOpHistogram;
import com.yammer.metrics.core.NoOpMeter;
import io.netty.buffer.ByteBuf;
import org.apache.giraph.metrics.MeterDesc;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Delegate Object to help keep track of the bytes processed and provide some
 * metrics when desired as part of the Netty Channel stack.
 */
public class ByteCounterDelegate implements ByteCounter {
  /** Megabyte in bytes */
  public static final double MEGABYTE = 1024f * 1024f;
  /** Helper to format the doubles */
  private static final DecimalFormat DOUBLE_FORMAT =
      new DecimalFormat("#######.####");
  /** Class timer */
  private static final Time TIME = SystemTime.get();
  /** Bytes processed during the most recent time interval */
  private final AtomicLong bytesProcessed = new AtomicLong();
  /** Aggregate bytes per superstep */
  private final AtomicLong bytesProcessedPerSuperstep = new AtomicLong();
  /** Total processed requests */
  private final AtomicLong processedRequests = new AtomicLong();
  /** Start time (for bandwidth calculation) */
  private final AtomicLong startMsecs = new AtomicLong();
  /** Last updated msecs for getMetricsWindow */
  private final AtomicLong metricsWindowLastUpdatedMsecs = new AtomicLong();

  // Metrics
  /** Meter of requests sent */
  private Meter processedRequestsMeter = NoOpMeter.INSTANCE;
  /** Histogram of bytes sent */
  private Histogram processedBytesHist = NoOpHistogram.INSTANCE;

  /** Is it delegate for InBoundByteCounter */
  private final boolean isInbound;

  /**
   * Constructor to specify if delegate is created by InBound/ Outbound counter
   *
   * @param isInBound switch to specify if instantiated by inbound counter
   */
  public ByteCounterDelegate(boolean isInBound) {
    this.isInbound = isInBound;
  }

  /**
   * Called by Inbound/ Outbound counters to refresh meters on a new superstep
   *
   * @param superstepMetrics superstepmetrics registry
   */
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    if (isInbound) {
      processedRequestsMeter = superstepMetrics.getMeter(
          MeterDesc.RECEIVED_REQUESTS);
      processedBytesHist = superstepMetrics.getUniformHistogram(
          MetricNames.RECEIVED_BYTES);
    } else {
      processedRequestsMeter = superstepMetrics.getMeter(
          MeterDesc.SENT_REQUESTS);
      processedBytesHist = superstepMetrics.getUniformHistogram(
          MetricNames.SENT_BYTES);
    }
  }

  /**
   * Updates properties based on bytes sent / received
   *
   * @param buf ByteBuf received by the counter
   * @return number of readable bytes
   */
  public int byteBookkeeper(ByteBuf buf) {
    int processedBytes = buf.readableBytes();
    bytesProcessed.addAndGet(processedBytes);
    bytesProcessedPerSuperstep.addAndGet(processedBytes);
    processedBytesHist.update(processedBytes);
    processedRequests.incrementAndGet();
    processedRequestsMeter.mark();
    return processedBytes;
  }

  /**
   * Reset all the bytes kept track of.
   */
  public void resetBytes() {
    bytesProcessed.set(0);
    processedRequests.set(0);
  }

  /**
   * Reset the start msecs.
   */
  public void resetStartMsecs() {
    startMsecs.set(TIME.getMilliseconds());
  }

  @Override
  public void resetAll() {
    resetBytes();
    resetStartMsecs();
  }

  /**
   * Returns bytes processed per superstep.
   * @return Number of bytes.
   */
  public long getBytesProcessedPerSuperstep() {
    return bytesProcessedPerSuperstep.get();
  }

  /**
   * Set bytes processed per superstep to 0.
   */
  public void resetBytesProcessedPerSuperstep() {
    bytesProcessedPerSuperstep.set(0);
  }

  public long getBytesProcessed() {
    return bytesProcessed.get();
  }

  /**
   * @return Mbytes processed / sec in the current interval
   */
  public double getMbytesPerSecProcessed() {
    return bytesProcessed.get() * 1000f /
        (1 + TIME.getMilliseconds() - startMsecs.get()) / MEGABYTE;
  }

  /**
   * Helper method used by getMetrics to create its return string
   * @param mBytesProcessed mbytes processed
   * @param mBytesProcessedPerReq mbytes processed per request
   * @return A string containing all the metrics
   */
  public String getMetricsHelper(double mBytesProcessed,
                                 double mBytesProcessedPerReq) {
    if (isInbound) {
      return "MBytes/sec received = " +
          DOUBLE_FORMAT.format(getMbytesPerSecProcessed()) +
          ", MBytesReceived = " + DOUBLE_FORMAT.format(mBytesProcessed) +
          ", ave received req MBytes = " +
          DOUBLE_FORMAT.format(mBytesProcessedPerReq) +
          ", secs waited = " +
          ((TIME.getMilliseconds() - startMsecs.get()) / 1000f);
    } else {
      return "MBytes/sec sent = " +
          DOUBLE_FORMAT.format(getMbytesPerSecProcessed()) +
          ", MBytesSent = " + DOUBLE_FORMAT.format(mBytesProcessed) +
          ", ave sent req MBytes = " +
          DOUBLE_FORMAT.format(mBytesProcessedPerReq) +
          ", secs waited = " +
          ((TIME.getMilliseconds() - startMsecs.get()) / 1000f);
    }
  }

  @Override
  public String getMetrics() {
    double mBytesProcessed = bytesProcessed.get() / MEGABYTE;
    long curProcessedRequests = processedRequests.get();
    double mBytesProcessedPerReq = (curProcessedRequests == 0) ? 0 :
        mBytesProcessed / curProcessedRequests;

    return getMetricsHelper(mBytesProcessed, mBytesProcessedPerReq);
  }

  @Override
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
