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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;

/**
 * Observer for worker. The user can subclass and register an observer with the
 * Giraph framework. The framework will execute methods of the observer at
 * designated moments of computation on each worker.
 * It can implement ContextSettable if it needs to access job counters.
 */
public interface WorkerObserver extends ImmutableClassesGiraphConfigurable {
  /**
   * Initialize the observer. This method is executed once on each worker before
   * loading.
   */
  void preLoad();

  /**
   * Initialize the observer. This method is executed once on each worker after
   * loading before the first superstep starts.
   */
  void preApplication();

  /**
   * Execute the observer. This method is executed once on each worker before
   * each superstep starts.
   *
   * @param superstep number of superstep
   */
  void preSuperstep(long superstep);

  /**
   * Execute the observer. This method is executed once on each worker after
   * each superstep ends.
   *
   * @param superstep number of superstep
   */
  void postSuperstep(long superstep);

  /**
   * Finalize the observer. This method is executed once on each worker after
   * the last superstep ends before saving.
   */
  void postApplication();

  /**
   * Finalize the observer. This method is executed once on each worker after
   * saving.
   */
  void postSave();
}
