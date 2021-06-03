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

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.yourkit.api.Controller;
import com.yourkit.api.ProfilingModes;

import java.io.File;
import java.io.IOException;

/**
 * Convenience context for profiling. Hides away all of the exception handling.
 * Do not instantiate directly, use only through {@link YourKitProfiler}.
 */
public class YourKitContext {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(YourKitContext.class);

  /** Joiner on path separator */
  private static Joiner SLASH_JOINER = Joiner.on("/");

  /** The YourKit profiling object, or null if no profiling going on. */
  private final Controller yourKitController;

  /**
   * Constructor
   * @param yourKitController profiling object
   */
  YourKitContext(Controller yourKitController) {
    this.yourKitController = yourKitController;
  }

  /**
   * Capture a snapshot
   * @param flags See {@link com.yourkit.api.ProfilingModes}
   * @param destPath where to store snapshot
   */
  private void snapshot(long flags, String destPath) {
    if (yourKitController != null) {
      String path;
      try {
        path = yourKitController.captureSnapshot(flags);
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        return;
      }
      try {
        File destFile = new File(destPath);
        Files.createParentDirs(destFile);
        Files.move(new File(path), destFile);
      } catch (IOException e) {
        LOG.error("Failed to move YourKit snapshot file from " + path +
            " to " + destPath, e);
      }
    }
  }

  /**
   * Capture snapshot with all recorded data including heap dump.
   *
   * WARNING: This is likely to be VERY slow for large jobs.
   *
   * @param destPath path to store snapshot file
   */
  public void snapshotWithMemory(String destPath) {
    snapshot(ProfilingModes.SNAPSHOT_WITH_HEAP, destPath);
  }

  /**
   * Capture snapshot with all recorded data including heap dump.
   * The snapshot file is saved in log directory, or /tmp as default.
   *
   * WARNING: This is likely to be VERY slow for large jobs.
   *
   * @param context context
   * @param name    snapshot file name
   */
  public void snapshotWithMemory(Mapper.Context context, String name) {
    snapshot(ProfilingModes.SNAPSHOT_WITH_HEAP,
        SLASH_JOINER.join(System.getProperty("hadoop.log.dir", "/tmp"),
            "userlogs", context.getTaskAttemptID(), name + ".snapshot"));
  }

  /**
   * Capture snapshot with all recorded data except for heap dump.
   *
   * @param destPath path to store snapshot file
   */
  public void snapshotCPUOnly(String destPath) {
    snapshot(ProfilingModes.SNAPSHOT_WITHOUT_HEAP, destPath);
  }

  /**
   * Capture snapshot with all recorded data except for heap dump.
   * The snapshot file is saved in log directory, or /tmp as default.
   *
   * @param context context
   * @param name    snapshot file name
   */
  public void snapshotCPUOnly(Mapper.Context context, String name) {
    snapshot(ProfilingModes.SNAPSHOT_WITHOUT_HEAP,
        SLASH_JOINER.join(System.getProperty("hadoop.log.dir", "/tmp"),
            "userlogs", context.getTaskAttemptID(), name + ".snapshot"));
  }

  /**
   * Stop recording
   */
  public void stop() {
    if (yourKitController != null) {
      try {
        yourKitController.disableStackTelemetry();
        LOG.info("Disabled YourKit stack telemetry");
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Failed to stop stack telemetry", e);
      }

      try {
        yourKitController.stopCPUProfiling();
        LOG.info("Stopped Yourkit CPU profiling");
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Failed to stop YourKit profiling", e);
      }

      try {
        yourKitController.stopAllocationRecording();
        LOG.info("Stopped Yourkit allocation recording");
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Failed to stop YourKit allocation recording", e);
      }
    }
  }
}
