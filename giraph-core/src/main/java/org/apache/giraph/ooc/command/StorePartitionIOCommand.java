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

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.ooc.data.DiskBackedEdgeStore;
import org.apache.giraph.ooc.data.DiskBackedMessageStore;
import org.apache.giraph.ooc.data.DiskBackedPartitionStore;
import org.apache.giraph.ooc.OutOfCoreEngine;

import java.io.IOException;

/**
 * IOCommand to store partition data, edge data (if in INPUT_SUPERSTEP), and
 * message data (if in compute supersteps).
 */
public class StorePartitionIOCommand extends IOCommand {
  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param partitionId id of the partition to store its data
   */
  public StorePartitionIOCommand(OutOfCoreEngine oocEngine,
                                 int partitionId) {
    super(oocEngine, partitionId);
  }

  @Override
  public boolean execute() throws IOException {
    boolean executed = false;
    if (oocEngine.getMetaPartitionManager()
        .startOffloadingPartition(partitionId)) {
      DiskBackedPartitionStore partitionStore =
          (DiskBackedPartitionStore)
              oocEngine.getServerData().getPartitionStore();
      numBytesTransferred +=
          partitionStore.offloadPartitionData(partitionId);
      if (oocEngine.getSuperstep() != BspService.INPUT_SUPERSTEP) {
        MessageStore messageStore =
            oocEngine.getServerData().getCurrentMessageStore();
        if (messageStore != null) {
          numBytesTransferred += ((DiskBackedMessageStore) messageStore)
              .offloadPartitionData(partitionId);
        }
      } else {
        DiskBackedEdgeStore edgeStore =
            (DiskBackedEdgeStore)
                oocEngine.getServerData().getEdgeStore();
        numBytesTransferred +=
            edgeStore.offloadPartitionData(partitionId);
      }
      oocEngine.getMetaPartitionManager().doneOffloadingPartition(partitionId);
      executed = true;
    }
    return executed;
  }

  @Override
  public IOCommandType getType() {
    return IOCommandType.STORE_PARTITION;
  }

  @Override
  public String toString() {
    return "StorePartitionIOCommand: (partitionId = " + partitionId + ")";
  }
}
