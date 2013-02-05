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

package org.apache.giraph.worker;

/**
 * Observer for Worker
 */
public interface WorkerObserver {
  /**
   * Initialize the WorkerContext.
   * This method is executed once on each Worker before the first
   * superstep starts.
   */
  void preApplication();

  /**
   * Finalize the WorkerContext.
   * This method is executed once on each Worker after the last
   * superstep ends.
   */
  void postApplication();

  /**
   * Execute user code.
   * This method is executed once on each Worker before each
   * superstep starts.
   *
   * @param superstep number of superstep
   */
  void preSuperstep(long superstep);

  /**
   * Execute user code.
   * This method is executed once on each Worker after each
   * superstep ends.
   *
   * @param superstep number of superstep
   */
  void postSuperstep(long superstep);
}
