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

import org.apache.log4j.Logger;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictor;
import org.jboss.netty.channel.ReceiveBufferSizePredictor;
import org.jboss.netty.channel.ReceiveBufferSizePredictorFactory;

/**
 * Uses composition to learn more about what
 * AdaptiveReceiveBufferSizePredictor has determined what the actual
 * sizes are.
 */
public class WrappedAdaptiveReceiveBufferSizePredictorFactory implements
    ReceiveBufferSizePredictorFactory {
  /** Internal predictor */
  private final ReceiveBufferSizePredictor receiveBufferSizePredictor;

  /**
   * Constructor with defaults.
   */
  public WrappedAdaptiveReceiveBufferSizePredictorFactory() {
    receiveBufferSizePredictor =
        new WrappedAdaptiveReceiveBufferSizePredictor();
  }

  /**
   * Creates a new predictor with the specified parameters.
   *
   * @param minimum The inclusive lower bound of the expected buffer size
   * @param initial The initial buffer size when no feed back was received
   * @param maximum The inclusive upper bound of the expected buffer size
   */
  public WrappedAdaptiveReceiveBufferSizePredictorFactory(int minimum,
                                                          int initial,
                                                          int maximum) {
    receiveBufferSizePredictor = new WrappedAdaptiveReceiveBufferSizePredictor(
        minimum, initial, maximum);
  }

  @Override
  public ReceiveBufferSizePredictor getPredictor() throws Exception {
    return receiveBufferSizePredictor;
  }

  /**
   * Uses composition to expose
   * details of AdaptiveReceiveBufferSizePredictor.
   */
  private static class WrappedAdaptiveReceiveBufferSizePredictor implements
      ReceiveBufferSizePredictor {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(
        WrappedAdaptiveReceiveBufferSizePredictor.class);
    /** Internally delegated predictor */
    private final AdaptiveReceiveBufferSizePredictor
    adaptiveReceiveBufferSizePredictor;
    /** Number of calls to nextReceiveBufferSize()  */
    private long nextReceiveBufferSizeCount = 0;
    /** Number of calls to previousReceiveBufferSize()  */
    private long previousReceiveBufferSizeCount = 0;

    /**
     * Creates a new predictor with the default parameters.  With the default
     * parameters, the expected buffer size starts from {@code 1024}, does not
     * go down below {@code 64}, and does not go up above {@code 65536}.
     */
    public WrappedAdaptiveReceiveBufferSizePredictor() {
      adaptiveReceiveBufferSizePredictor =
          new AdaptiveReceiveBufferSizePredictor();
    }

    /**
     * Creates a new predictor with the specified parameters.
     *
     * @param minimum  the inclusive lower bound of the expected buffer size
     * @param initial  the initial buffer size when no feed back was received
     * @param maximum  the inclusive upper bound of the expected buffer size
     */
    public WrappedAdaptiveReceiveBufferSizePredictor(int minimum,
                                                     int initial,
                                                     int maximum) {
      adaptiveReceiveBufferSizePredictor =
          new AdaptiveReceiveBufferSizePredictor(minimum, initial, maximum);
    }

    @Override
    public int nextReceiveBufferSize() {
      int nextReceiveBufferSize =
          adaptiveReceiveBufferSizePredictor.nextReceiveBufferSize();
      if (LOG.isDebugEnabled()) {
        if (nextReceiveBufferSizeCount % 1000 == 0) {
          LOG.debug("nextReceiveBufferSize: size " +
              nextReceiveBufferSize +            " " +
              "count " + nextReceiveBufferSizeCount);
        }
        ++nextReceiveBufferSizeCount;
      }
      return nextReceiveBufferSize;
    }

    @Override
    public void previousReceiveBufferSize(int previousReceiveBufferSize) {
      if (LOG.isDebugEnabled()) {
        if (previousReceiveBufferSizeCount % 1000 == 0) {
          LOG.debug("previousReceiveBufferSize: size " +
              previousReceiveBufferSize +
              ", count " + previousReceiveBufferSizeCount);
        }
        ++previousReceiveBufferSizeCount;
      }
      adaptiveReceiveBufferSizePredictor.previousReceiveBufferSize(
          previousReceiveBufferSize);
    }
  }
}
