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

package org.apache.giraph.zk;

/**
 * Synchronize on waiting for an event to have happened.  This is a one-time
 * event.
 */
public interface BspEvent {
  /**
   * Reset the permanent signal.
   */
  void reset();

  /**
   * The event occurred and the occurrence has been logged for future
   * waiters.
   */
  void signal();

  /**
   * Wait until the event occurred or waiting timed out.
   * @param msecs Milliseconds to wait until the event occurred. 0 indicates
   *        check immediately.  -1 indicates wait forever.
   * @return true if event occurred, false if timed out while waiting
   */
  boolean waitMsecs(int msecs);

  /**
   * Waits until timeout or fails with runtime exception.
   * @param timeout Throws exception if waiting takes longer than timeout.
   */
  void waitForTimeoutOrFail(long timeout);
}
