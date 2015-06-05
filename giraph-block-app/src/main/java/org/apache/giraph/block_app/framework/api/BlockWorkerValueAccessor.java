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
package org.apache.giraph.block_app.framework.api;

/**
 * Function for accessing WorkerValue.
 *
 * Intentionally hidden from APIs, to allow usage only
 * within PieceWithWorkerContext.
 */
public interface BlockWorkerValueAccessor {
  /**
   * Get global worker value.
   * Value returned can be accessed from may threads, and so all
   * accesses to it should be done in a thread-safe manner!
   *
   * This is the only place in Blocks Framework where you need
   * to take care of concurrency.
   */
  Object getWorkerValue();
}
