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

import org.apache.giraph.conf.GiraphConfiguration;
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
 * This class is a simple helper around the API mentioned above that allows you
 * to easily wrap code with
 * {@link YourKitProfiler#startProfile(GiraphConfiguration)}
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

  /** Don't construct, allow inheritance */
  protected YourKitProfiler() { }

  /**
   * Convenient replacement of {@link #startProfilingCPU(long)} with
   * {@link ProfilingModes#CPU_TRACING} for the mode.
   *
   * @param conf GiraphConfiguration
   * @return profiler context
   */
  public static YourKitContext startProfile(GiraphConfiguration conf) {
    Controller controller = null;
    try {
      controller = new Controller();
      controller.enableStackTelemetry();
      controller.startCPUProfiling(ProfilingModes.CPU_SAMPLING,
          Controller.DEFAULT_FILTERS);
      LOG.debug("Started YourKit profiling CPU");
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.debug("Failed to start YourKit CPU profiling", e);
    }
    return new YourKitContext(controller);
  }
}
