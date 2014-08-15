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

package org.apache.giraph.bsp;

/**
 * State of a coordinated superstep
 */
public enum SuperstepState {
  /** Nothing happened yet */
  INITIAL(false),
  /** A worker died during this superstep */
  WORKER_FAILURE(false),
  /** This superstep completed correctly */
  THIS_SUPERSTEP_DONE(false),
  /** All supersteps are complete */
  ALL_SUPERSTEPS_DONE(true),
  /** Execution halted */
  CHECKPOINT_AND_HALT(true);

  /** Should we stop execution after this superstep? */
  private boolean executionComplete;

  /**
   * Enum constructor
   * @param executionComplete is final state?
   */
  SuperstepState(boolean executionComplete) {
    this.executionComplete = executionComplete;
  }

  /**
   * Returns true if execution has to be stopped after this
   * superstep.
   * @return whether execution is complete.
   */
  public boolean isExecutionComplete() {
    return executionComplete;
  }
}
