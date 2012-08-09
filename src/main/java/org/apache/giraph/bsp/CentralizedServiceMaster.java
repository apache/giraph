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

package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.giraph.graph.MasterAggregatorUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.zookeeper.KeeperException;

/**
 * At most, there will be one active master at a time, but many threads can
 * be trying to be the active master.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public interface CentralizedServiceMaster<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> extends
    CentralizedService<I, V, E, M>, MasterAggregatorUsage {
  /**
   * Become the master.
   * @return true if became the master, false if the application is done.
   */
  boolean becomeMaster();

  /**
   * Create the {@link InputSplit} objects from the index range based on the
   * user-defined VertexInputFormat.  The {@link InputSplit} objects will
   * processed by the workers later on during the INPUT_SUPERSTEP.
   *
   * @return Number of partitions. Returns -1 on failure to create
   *         valid input splits.
   */
  int createInputSplits();

  /**
   * Master coordinates the superstep
   *
   * @return State of the application as a result of this superstep
   * @throws InterruptedException
   * @throws KeeperException
   */
  SuperstepState coordinateSuperstep()
    throws KeeperException, InterruptedException;

  /**
   * Master can decide to restart from the last good checkpoint if a
   * worker fails during a superstep.
   *
   * @param checkpoint Checkpoint to restart from
   */
  void restartFromCheckpoint(long checkpoint);

  /**
   * Get the last known good checkpoint
   *
   * @return Last good superstep number
   * @throws IOException
   */
  long getLastGoodCheckpoint() throws IOException;

  /**
   * If the master decides that this job doesn't have the resources to
   * continue, it can fail the job.  It can also designate what to do next.
   * Typically this is mainly informative.
   *
   * @param state State of the application.
   * @param applicationAttempt Attempt to start on
   * @param desiredSuperstep Superstep to restart from (if applicable)
   */
  void setJobState(ApplicationState state,
    long applicationAttempt,
    long desiredSuperstep);
}
