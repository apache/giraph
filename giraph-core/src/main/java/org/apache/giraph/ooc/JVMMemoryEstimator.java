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
import org.apache.giraph.utils.MemoryUtils;

/**
 * Memory estimator class using JVM runtime methods to estimate the
 * free/available memory.
 */
public class JVMMemoryEstimator implements MemoryEstimator {
  /**
   * Constructor for reflection
   */
  public JVMMemoryEstimator() { }

  @Override
  public void initialize(CentralizedServiceWorker serviceWorker) { }

  @Override
  public double freeMemoryMB() {
    return MemoryUtils.freePlusUnallocatedMemoryMB();
  }

  @Override public double maxMemoryMB() {
    return MemoryUtils.maxMemoryMB();
  }
}
