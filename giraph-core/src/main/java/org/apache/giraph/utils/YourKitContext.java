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
   * @param context map context
   * @param name unique name for this snapshot
   */
  private void snapshot(long flags, Mapper.Context context, String name) {
    if (yourKitController != null) {
      String path;
      try {
        path = yourKitController.captureSnapshot(flags);
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        return;
      }
      File destFile = new File(SLASH_JOINER.join(
          "/tmp", context.getJobID(), context.getTaskAttemptID(),
          name + ".snapshot"));
      try {
        Files.createParentDirs(destFile);
        Files.move(new File(path), destFile);
      } catch (IOException e) {
        LOG.error("Failed to move YourKit snapshot file from " + path +
            " to " + destFile.getPath(), e);
      }
    }
  }

  /**
   * This method is just a convenient replacement of
   * {@link #captureSnapshot(long, java.io.File)} with
   * {@link com.yourkit.api.ProfilingModes#SNAPSHOT_WITH_HEAP} for the flags.
   *
   * WARNING: This is likely to be VERY slow for large jobs.
   *
   * @param context map context
   * @param name unique name for this snapshot
   */
  public void snapshotWithMemory(Mapper.Context context, String name) {
    snapshot(ProfilingModes.SNAPSHOT_WITH_HEAP, context, name);
  }

  /**
   * This method is just a convenient replacement of
   * {@link #captureSnapshot(long, java.io.File)} with
   * {@link com.yourkit.api.ProfilingModes#SNAPSHOT_WITHOUT_HEAP} for the flags.
   *
   * @param context map context
   * @param name unique name for this snapshot
   */
  public void snapshotCPUOnly(Mapper.Context context, String name) {
    snapshot(ProfilingModes.SNAPSHOT_WITHOUT_HEAP, context, name);
  }

  /**
   * Stop profiling CPU
   */
  public void stop() {
    if (yourKitController != null) {
      try {
        yourKitController.stopCPUProfiling();
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Failed to stop YourKit CPU profiling", e);
      }
    }
  }
}
