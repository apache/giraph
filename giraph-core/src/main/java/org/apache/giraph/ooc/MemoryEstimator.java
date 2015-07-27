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

package org.apache.giraph.ooc;

import org.apache.giraph.bsp.CentralizedServiceWorker;

/**
 * Interface for memory estimator. Estimated memory is used in adaptive
 * out-of-core mechanism.
 */
public interface MemoryEstimator {
  /**
   * Initialize the memory estimator.
   *
   * @param serviceWorker Worker service
   */
  void initialize(CentralizedServiceWorker serviceWorker);

  /**
   * @return amount of free memory in MB
   */
  double freeMemoryMB();

  /**
   * @return amount of available memory in MB
   */
  double maxMemoryMB();
}
