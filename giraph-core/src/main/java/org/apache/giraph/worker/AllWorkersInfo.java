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

import java.util.ArrayList;
import java.util.List;

import org.python.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Information about all workers, their WorkerInfo values, and indices.
 */
public class AllWorkersInfo {
  /** List of workers in current superstep, sorted by task id */
  private final List<WorkerInfo> workerInfos;
  /** My worker index */
  private final int myWorkerIndex;
  /** Map of taskId to worker index */
  private final Int2IntOpenHashMap taskIdToIndex;

  /**
   * Constructor
   * @param workers Ordered list of workers
   * @param myWorker My worker
   */
  public AllWorkersInfo(List<WorkerInfo> workers, WorkerInfo myWorker) {
    workerInfos = new ArrayList<>(workers);

    taskIdToIndex = new Int2IntOpenHashMap(workerInfos.size());
    taskIdToIndex.defaultReturnValue(-1);
    for (int i = 0; i < workerInfos.size(); i++) {
      int task = workerInfos.get(i).getTaskId();
      if (i > 0) {
        Preconditions.checkState(
            task > workerInfos.get(i - 1).getTaskId(), "Tasks not ordered");
      }
      Preconditions.checkState(task >= 0, "Task not specified, %d", task);
      int old = taskIdToIndex.put(task, i);
      Preconditions.checkState(old == -1,
          "Task with %d id found twice (positions %d and %d)",
          task, i, old);
    }

    myWorkerIndex = getWorkerIndex(myWorker);
  }

  /**
   * List of WorkerInfos
   *
   * @return List of WorkerInfos
   */
  public List<WorkerInfo> getWorkerList() {
    return workerInfos;
  }

  /**
   * Get number of workers
   *
   * @return Number of workers
   */
  public int getWorkerCount() {
    return workerInfos.size();
  }

  /**
   * Get index for this worker
   *
   * @return Index of this worker
   */
  public int getMyWorkerIndex() {
    return myWorkerIndex;
  }

  /**
   * For every worker this method returns unique number
   * between 0 and N, where N is the total number of workers.
   * This number stays the same throughout the computation.
   * TaskID may be different from this number and task ID
   * is not necessarily continuous
   * @param workerInfo worker info object
   * @return worker number
   */
  public int getWorkerIndex(WorkerInfo workerInfo) {
    return taskIdToIndex.get(workerInfo.getTaskId());
  }
}
