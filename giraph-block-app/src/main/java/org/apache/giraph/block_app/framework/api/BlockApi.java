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
 * Basic block computation API for accessing items
 * present on both workers and master.
 */
public interface BlockApi extends BlockConfApi {
 /**
   * Get number of workers
   *
   * @return Number of workers
   */
  int getWorkerCount();

  /**
   * Get the total (all workers) number of vertices that
   * existed at the start of the current piece.
   *
   * Recommended to avoid it, as it introduces global dependencies,
   * code will not be able to work on top of a subgraphs any more.
   * This number should be easily computable via reducer or aggregator.
   */
  @Deprecated
  long getTotalNumVertices();

  /**
   * Get the total (all workers) number of edges that
   * existed at the start of the current piece.
   *
   * Recommended to avoid it, as it introduces global dependencies,
   * code will not be able to work on top of a subgraphs any more.
   * This number should be easily computable via reducer or aggregator.
   */
  @Deprecated
  long getTotalNumEdges();
}
