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

import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a semaphore where number of available permits can change
 */
public final class AdjustableSemaphore extends Semaphore {
  /** Maximum number of available permits */
  private int maxPermits;

  /**
   * Constructor
   * @param permits initial number of available permits
   */
  public AdjustableSemaphore(int permits) {
    super(permits);
    maxPermits = permits;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      "UG_SYNC_SET_UNSYNC_GET")
  public int getMaxPermits() {
    return maxPermits;
  }

  /**
   * Adjusts the maximum number of available permits.
   *
   * @param newMax max number of permits
   */
  public synchronized void setMaxPermits(int newMax) {
    checkState(newMax >= 0, "setMaxPermits: number of permits cannot be " +
        "less than 0");
    int delta = newMax - this.maxPermits;
    if (delta > 0) {
      // Releasing semaphore to make room for 'delta' more permits
      release(delta);
    } else if (delta < 0) {
      // Reducing number of permits in the semaphore
      reducePermits(-delta);
    }
    this.maxPermits = newMax;
  }
}
