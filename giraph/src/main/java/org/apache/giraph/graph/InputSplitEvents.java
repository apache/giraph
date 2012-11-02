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

package org.apache.giraph.graph;

import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.util.Progressable;

/**
 * Simple container of input split events.
 */
public class InputSplitEvents {
  /** Input splits are ready for consumption by workers */
  private final BspEvent allReadyChanged;
  /** Input split reservation or finished notification and synchronization */
  private final BspEvent stateChanged;
  /** Input splits are done being processed by workers */
  private final BspEvent allDoneChanged;
  /** Input split done by a worker finished notification and synchronization */
  private final BspEvent doneStateChanged;

  /**
   * Constructor.
   *
   * @param progressable {@link Progressable} to report progress
   */
  public InputSplitEvents(Progressable progressable) {
    allReadyChanged = new PredicateLock(progressable);
    stateChanged = new PredicateLock(progressable);
    allDoneChanged = new PredicateLock(progressable);
    doneStateChanged = new PredicateLock(progressable);
  }

  /**
   * Get event for input splits all ready
   *
   * @return {@link BspEvent} for input splits all ready
   */
  public BspEvent getAllReadyChanged() {
    return allReadyChanged;
  }

  /**
   * Get event for input splits state
   *
   * @return {@link BspEvent} for input splits state
   */
  public BspEvent getStateChanged() {
    return stateChanged;
  }

  /**
   * Get event for input splits all done
   *
   * @return {@link BspEvent} for input splits all done
   */
  public BspEvent getAllDoneChanged() {
    return allDoneChanged;
  }

  /**
   * Get event for input split done
   *
   * @return {@link BspEvent} for input split done
   */
  public BspEvent getDoneStateChanged() {
    return doneStateChanged;
  }
}
