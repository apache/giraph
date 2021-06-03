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

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.master.MasterAggregatorUsage;
import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.hadoop.io.Writable;

/**
 * Block computation API available for the master methods.
 *
 * Interface to the MasterCompute methods.
 */
public interface BlockMasterApi extends MasterAggregatorUsage,
    MasterGlobalCommUsage, StatusReporter, BlockApi, BlockOutputApi {
  /**
   * No need to use it, and introduces global dependencies.
   *
   * Store data locally within the piece, or use ObjectHolder.
   */
  @Deprecated
  @Override
  <A extends Writable>
  boolean registerPersistentAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass
  ) throws InstantiationException, IllegalAccessException;

  /**
   * Broadcast given value to all workers for next computation.
   * @param value Value to broadcast
   */
  <T extends Writable> BroadcastHandle<T> broadcast(T value);

  /**
   * Call this to log a line to command line of the job. Use in moderation -
   * it's a synchronous call to Job client
   *
   * @param line Line to print
   */
  void logToCommandLine(String line);
}
