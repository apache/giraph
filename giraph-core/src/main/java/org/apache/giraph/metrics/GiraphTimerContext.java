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

package org.apache.giraph.metrics;

import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;

import java.util.concurrent.TimeUnit;

/**
 * Context to use with GiraphTimer for timing an event.
 */
public class GiraphTimerContext {
  /** Object to use for time measurement */
  private static Time TIME = SystemTime.get();

  /** GiraphTimer to callback to with result of timing */
  private GiraphTimer giraphTimer;
  /** Beginning time to compare against */
  private long startTime;

  /**
   * Create with GiraphTimer to callback to.
   *
   * @param giraphTimer GiraphTimer to use.
   */
  public GiraphTimerContext(GiraphTimer giraphTimer) {
    this.giraphTimer = giraphTimer;
    this.startTime = TIME.getNanoseconds();
  }

  /**
   * End timing. Set time elapsed in GiraphTimer.
   */
  public void stop() {
    giraphTimer.set(Times.getNanosecondsSince(TIME, startTime),
                    TimeUnit.NANOSECONDS);
  }
}
