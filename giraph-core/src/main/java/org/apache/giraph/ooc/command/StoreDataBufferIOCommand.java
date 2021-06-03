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

package org.apache.giraph.ooc.command;

import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.data.DiskBackedEdgeStore;
import org.apache.giraph.ooc.data.DiskBackedMessageStore;
import org.apache.giraph.ooc.data.DiskBackedPartitionStore;

import java.io.IOException;

/**
 * IOCommand to store raw data buffers on disk.
 */
public class StoreDataBufferIOCommand extends IOCommand {
  /**
   * Types of raw data buffer to offload to disk (either vertices/edges buffer
   * in INPUT_SUPERSTEP or incoming message buffer).
   */
  public enum DataBufferType { PARTITION, MESSAGE };
  /**
   * Type of the buffer to store on disk.
   */
  private final DataBufferType type;

  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param partitionId id of the partition to offload its buffers
   * @param type type of the buffer to store on disk
   */
  public StoreDataBufferIOCommand(OutOfCoreEngine oocEngine,
                                  int partitionId,
                                  DataBufferType type) {
    super(oocEngine, partitionId);
    this.type = type;
  }

  @Override
  public boolean execute() throws IOException {
    boolean executed = false;
    if (oocEngine.getMetaPartitionManager()
        .startOffloadingBuffer(partitionId)) {
      switch (type) {
      case PARTITION:
        DiskBackedPartitionStore partitionStore =
            (DiskBackedPartitionStore)
                oocEngine.getServerData().getPartitionStore();
        numBytesTransferred +=
            partitionStore.offloadBuffers(partitionId);
        DiskBackedEdgeStore edgeStore =
            (DiskBackedEdgeStore) oocEngine.getServerData().getEdgeStore();
        numBytesTransferred += edgeStore.offloadBuffers(partitionId);
        break;
      case MESSAGE:
        DiskBackedMessageStore messageStore =
            (DiskBackedMessageStore)
                oocEngine.getServerData().getIncomingMessageStore();
        numBytesTransferred +=
            messageStore.offloadBuffers(partitionId);
        break;
      default:
        throw new IllegalStateException("execute: requested data buffer type " +
            "does not exist!");
      }
      oocEngine.getMetaPartitionManager().doneOffloadingBuffer(partitionId);
      executed = true;
    }
    return executed;
  }

  @Override
  public IOCommandType getType() {
    return IOCommandType.STORE_BUFFER;
  }

  @Override
  public String toString() {
    return "StoreDataBufferIOCommand: (partitionId = " + partitionId + ", " +
        "type = " + type.name() + ")";
  }
}
