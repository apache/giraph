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

package org.apache.giraph.utils;

import org.apache.log4j.Logger;

import com.yourkit.api.Controller;
import com.yourkit.api.ProfilingModes;

/**
 * Helper for YourKit profiling from within the code.
 *
 * See the following for information about usage:
 *  - http://www.yourkit.com/docs/95/help/api.jsp
 *  - http://www.yourkit.com/docs/95/api/index.html
 *
 * This class is a simple helper around the API mentioned above
 * followed by any amount of snapshotX calls and finally
 * {@link YourKitContext#stop()}.
 * See also {@link YourKitContext}.
 *
 * As of 05/2013 YourKit is not publishing their API jars to Maven, but their
 * license allows us to do it, so we have setup a repository to do this.
 * See https://github.com/facebook/sonatype-yourkit for more info.
 */
public class YourKitProfiler {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(YourKitProfiler.class);
  /** Record every ALLOCATION_RECORDING_INTERVAL'th allocation */
  private static final int ALLOCATION_RECORDING_INTERVAL = 1000;

  /** Don't construct, allow inheritance */
  protected YourKitProfiler() { }

  /**
   * Create a YourKit controller and do some or all of
   * {@link Controller#enableExceptionTelemetry()}
   * {@link Controller#startCPUProfiling(long, String, String)}
   * {@link Controller#startAllocationRecording(boolean, int, boolean,
   * int, boolean, boolean)}
   * based on boolean config options passed as method parameters
   *
   * @param enableStackTelemetry      enable stack telementry
   * @param enableCPUProfilling       enable CPU profilling
   * @param enableAllocationRecording enable allocation recording
   *
   * @return profiler context, or null if controller cannot be created
   */
  public static YourKitContext startProfile(boolean enableStackTelemetry,
                                            boolean enableCPUProfilling,
                                            boolean enableAllocationRecording) {
    Controller controller;
    try {
      controller = new Controller();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.info("Failed to set up YourKit controller", e);
      return null;
    }

    try {
      if (enableStackTelemetry) {
        controller.enableStackTelemetry();
        LOG.info("Enabled Yourkit stack telemetry");
      }
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.info("Failed to enable YourKit stack telemetry", e);
    }

    try {
      if (enableCPUProfilling) {
        controller.startCPUProfiling(ProfilingModes.CPU_SAMPLING,
          Controller.DEFAULT_FILTERS, Controller.DEFAULT_WALLTIME_SPEC);
        LOG.info("Started YourKit CPU profiling");
      }
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.info("Failed to start YourKit CPU profiling", e);
    }

    try {
      if (enableAllocationRecording) {
        controller.startAllocationRecording(true, ALLOCATION_RECORDING_INTERVAL,
            false, -1, true, false);
        LOG.info("Started YourKit allocation recording");
      }
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.info("Failed to start YourKit allocation recording", e);
    }

    return new YourKitContext(controller);
  }
}
