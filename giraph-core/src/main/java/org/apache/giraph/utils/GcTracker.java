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

import org.apache.giraph.conf.IntConfOption;
import org.apache.hadoop.conf.Configuration;

import com.sun.management.GcInfo;

/**
 * Tracks last k GC pauses and is able to tell how much time was spent doing
 * GC since some point in time. Thread safe.
 */
public class GcTracker {
  /** How many last gcs to keep */
  public static final IntConfOption LAST_GCS_TO_KEEP =
      new IntConfOption("giraph.lastGcsToKeep", 100,
          "How many last gcs to keep");

  /** Last k GCs which happened */
  private final GcInfo[] lastKGcs;
  /** Next position to write gc to */
  private int positionInGcs = 0;

  /**
   * Constructor with default number of last gcs to keep
   */
  public GcTracker() {
    lastKGcs = new GcInfo[LAST_GCS_TO_KEEP.getDefaultValue()];
  }

  /**
   * Constructor with configuration
   *
   * @param conf Configuration
   */
  public GcTracker(Configuration conf) {
    lastKGcs = new GcInfo[LAST_GCS_TO_KEEP.get(conf)];
  }

  /**
   * Called to notify gc tracker that gc occurred
   *
   * @param gcInfo GC info
   */
  public synchronized void gcOccurred(GcInfo gcInfo) {
    lastKGcs[positionInGcs] = gcInfo;
    positionInGcs++;
    if (positionInGcs == lastKGcs.length) {
      positionInGcs = 0;
    }
  }

  /**
   * Check how much time was spent doing gc since some timestamp
   *
   * @param timeSinceJvmStarted Timestamp to measure from, you can use
   *     ManagementFactory.getRuntimeMXBean().getUptime() to get it
   * @return How much time was spent doing gc since that timestamp (if there
   * were more gcs than we are keeping (LAST_GCS_TO_KEEP) then it will be
   * partial result)
   */
  public synchronized long gcTimeSpentSince(long timeSinceJvmStarted) {
    long ret = 0;
    int currentPosition = positionInGcs;
    do {
      currentPosition--;
      if (currentPosition < 0) {
        currentPosition = lastKGcs.length - 1;
      }
      // If there was no such GC or GC started before the timestamp given,
      // stop iterating
      if (lastKGcs[currentPosition] == null ||
          lastKGcs[currentPosition].getStartTime() < timeSinceJvmStarted) {
        break;
      }
      ret += lastKGcs[currentPosition].getDuration();
    } while (currentPosition != positionInGcs);
    return ret;
  }
}
