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

package org.apache.giraph.master;

import org.apache.giraph.graph.TaskInfo;

/**
 * Information about the master that is sent to other workers.
 */
public class MasterInfo extends TaskInfo {
  /** Master task id is always -1 */
  public static final int MASTER_TASK_ID = -1;
  /**
   * Constructor
   */
  public MasterInfo() {
  }

  /**
   * This taskId is used internally as a unique identification of the
   * nettyServer which runs on this task. Master is always returning -1
   * because we need to make sure that the case when option
   * {@link org.apache.giraph.conf.GiraphConstants#SPLIT_MASTER_WORKER} is
   * false works correctly (master needs to have different id than the worker
   * which runs in the same mapper)
   *
   * @return -1
   */
  @Override
  public int getTaskId() {
    return MASTER_TASK_ID;
  }

  @Override
  public String toString() {
    return "Master(hostname=" + getHostname() +
        ", MRtaskID=" + super.getTaskId() +
        ", port=" + getPort() +
        ")";
  }
}
