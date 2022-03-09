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

import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.requests.AskForInputSplitRequest;
import org.apache.giraph.io.InputType;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Requests splits from master and keeps track of them
 */
public class WorkerInputSplitsHandler {
  /** Worker info of this worker */
  private final WorkerInfo workerInfo;
  /** Task id of master */
  private final int masterTaskId;
  /** Worker client, used for communication */
  private final WorkerClient workerClient;
  /** Map with currently available splits received from master */
  private final Map<InputType, BlockingQueue<byte[]>> availableInputSplits;

  /**
   * Constructor
   *
   * @param workerInfo   Worker info of this worker
   * @param masterTaskId Task id of master
   * @param workerClient Worker client, used for communication
   */
  public WorkerInputSplitsHandler(WorkerInfo workerInfo, int masterTaskId,
      WorkerClient workerClient) {
    this.workerInfo = workerInfo;
    this.masterTaskId = masterTaskId;
    this.workerClient = workerClient;
    availableInputSplits = new EnumMap<>(InputType.class);
    for (InputType inputType : InputType.values()) {
      availableInputSplits.put(
          inputType, new LinkedBlockingQueue<byte[]>());
    }
  }

  /**
   * Called when an input split has been received from master, adding it to
   * the map
   *
   * @param splitType            Type of split
   * @param serializedInputSplit Split
   */
  public void receivedInputSplit(InputType splitType,
      byte[] serializedInputSplit) {
    try {
      availableInputSplits.get(splitType).put(serializedInputSplit);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted", e);
    }
  }

  /**
   * Try to reserve an InputSplit for loading.  While InputSplits exists that
   * are not finished, wait until they are.
   *
   * NOTE: iterations on the InputSplit list only halt for each worker when it
   * has scanned the entire list once and found every split marked RESERVED.
   * When a worker fails, its Ephemeral RESERVED znodes will disappear,
   * allowing other iterating workers to claim it's previously read splits.
   * Only when the last worker left iterating on the list fails can a danger
   * of data loss occur. Since worker failure in INPUT_SUPERSTEP currently
   * causes job failure, this is OK. As the failure model evolves, this
   * behavior might need to change. We could add watches on
   * inputSplitFinishedNodes and stop iterating only when all these nodes
   * have been created.
   *
   * @param splitType Type of split
   * @param isFirstSplit Whether this is the first split input thread reads
   * @return reserved InputSplit or null if no unfinished InputSplits exist
   */
  public byte[] reserveInputSplit(InputType splitType, boolean isFirstSplit) {
    // Send request
    workerClient.sendWritableRequest(masterTaskId,
        new AskForInputSplitRequest(
            splitType, workerInfo.getTaskId(), isFirstSplit));
    try {
      // Wait for some split to become available
      byte[] serializedInputSplit = availableInputSplits.get(splitType).take();
      return serializedInputSplit.length == 0 ? null : serializedInputSplit;
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted", e);
    }
  }
}
