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

package org.apache.giraph.master.input;

import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Input splits organizer for vertex and edge input splits on master, which
 * uses locality information
 */
public class LocalityAwareInputSplitsMasterOrganizer
    implements InputSplitsMasterOrganizer {
  /** All splits before this pointer were taken */
  private final AtomicInteger listPointer = new AtomicInteger();
  /** List of serialized splits */
  private final List<byte[]> serializedSplits;
  /** Array containing information about whether a split was taken or not */
  private final AtomicBoolean[] splitsTaken;

  /** Map with preferred splits for each worker */
  private final Map<Integer, ConcurrentLinkedQueue<Integer>>
      workerToPreferredSplitsMap;


  /**
   * Constructor
   *
   * @param serializedSplits Serialized splits
   * @param splits           Splits
   * @param workers          List of workers
   */
  public LocalityAwareInputSplitsMasterOrganizer(List<byte[]> serializedSplits,
      List<InputSplit> splits, List<WorkerInfo> workers) {
    this.serializedSplits = serializedSplits;
    splitsTaken = new AtomicBoolean[serializedSplits.size()];
    // Mark all splits as not taken initially
    for (int i = 0; i < serializedSplits.size(); i++) {
      splitsTaken[i] = new AtomicBoolean(false);
    }

    workerToPreferredSplitsMap = new HashMap<>();
    for (WorkerInfo worker : workers) {
      workerToPreferredSplitsMap.put(worker.getTaskId(),
          new ConcurrentLinkedQueue<Integer>());
    }
    // Go through all splits
    for (int i = 0; i < splits.size(); i++) {
      try {
        String[] locations = splits.get(i).getLocations();
        // For every worker
        for (WorkerInfo worker : workers) {
          // Check splits locations
          for (String location : locations) {
            // If split is local for the worker, add it to preferred list
            if (location.contains(worker.getHostname())) {
              workerToPreferredSplitsMap.get(worker.getTaskId()).add(i);
              break;
            }
          }
        }
      } catch (IOException | InterruptedException e) {
        throw new IllegalStateException(
            "Exception occurred while getting splits locations", e);
      }
    }
  }

  @Override
  public byte[] getSerializedSplitFor(int workerTaskId) {
    ConcurrentLinkedQueue<Integer> preferredSplits =
        workerToPreferredSplitsMap.get(workerTaskId);
    // Try to find a local split
    while (true) {
      // Get position to check
      Integer splitIndex = preferredSplits.poll();
      // Check if all local splits were already processed for this worker
      if (splitIndex == null) {
        break;
      }
      // Try to reserve the split
      if (splitsTaken[splitIndex].compareAndSet(false, true)) {
        return serializedSplits.get(splitIndex);
      }
    }

    // No more local splits available, proceed linearly from splits list
    while (true) {
      // Get position to check
      int splitIndex = listPointer.getAndIncrement();
      // Check if all splits were already taken
      if (splitIndex >= serializedSplits.size()) {
        return null;
      }
      // Try to reserve the split
      if (splitsTaken[splitIndex].compareAndSet(false, true)) {
        return serializedSplits.get(splitIndex);
      }
    }
  }
}
