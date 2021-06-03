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
 * Interface providing utilities for using worker index.
 *
 * @param <I> Vertex id type
 */
public interface WorkerIndexUsage<I> {
  /**
   * Get number of workers
   *
   * @return Number of workers
   */
  int getWorkerCount();

  /**
   * Get index for this worker
   *
   * @return Index of this worker
   */
  int getMyWorkerIndex();

  /**
   * Get worker index which will contain vertex with given id,
   * if such vertex exists.
   *
   * @param vertexId vertex id
   * @return worker index
   */
  int getWorkerForVertex(I vertexId);
}
