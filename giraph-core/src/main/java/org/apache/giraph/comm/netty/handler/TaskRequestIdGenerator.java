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

package org.apache.giraph.comm.netty.handler;

import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generate different request ids based on the task id.  Thread-safe.
 */
public class TaskRequestIdGenerator {
  /** Task request generator map */
  private final ConcurrentMap<Integer, AtomicLong>
      taskRequestGeneratorMap = Maps.newConcurrentMap();

  /**
   * Get the next request id for a given destination.  Thread-safe.
   *
   * @param taskId id of the task(consistent during a superstep)
   * @return Valid request id
   */
  public Long getNextRequestId(Integer taskId) {
    AtomicLong requestGenerator = taskRequestGeneratorMap.get(taskId);
    if (requestGenerator == null) {
      requestGenerator = new AtomicLong(0);
      AtomicLong oldRequestGenerator =
          taskRequestGeneratorMap.putIfAbsent(taskId, requestGenerator);
      if (oldRequestGenerator != null) {
        requestGenerator = oldRequestGenerator;
      }
    }
    return requestGenerator.getAndIncrement();
  }
}
