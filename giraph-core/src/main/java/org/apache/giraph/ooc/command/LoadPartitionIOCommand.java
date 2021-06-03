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

import com.google.common.base.Preconditions;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.data.DiskBackedEdgeStore;
import org.apache.giraph.ooc.data.DiskBackedMessageStore;
import org.apache.giraph.ooc.data.DiskBackedPartitionStore;

import java.io.IOException;

/**
 * IOCommand to load partition data, edge data (if in INPUT_SUPERSTEP), and
 * message data (if in compute supersteps). Also, this command can be used to
 * prefetch a partition to be processed in the next superstep.
 */
public class LoadPartitionIOCommand extends IOCommand {
  /**
   * Which superstep this partition should be loaded for? (can be current
   * superstep or next superstep -- in case of prefetching).
   */
  private final long superstep;

  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param partitionId id of the partition to be loaded
   * @param superstep superstep to load the partition for
   */
  public LoadPartitionIOCommand(OutOfCoreEngine oocEngine, int partitionId,
                                long superstep) {
    super(oocEngine, partitionId);
    this.superstep = superstep;
  }

  @Override
  public boolean execute() throws IOException {
    boolean executed = false;
    if (oocEngine.getMetaPartitionManager()
        .startLoadingPartition(partitionId, superstep)) {
      long currentSuperstep = oocEngine.getSuperstep();
      DiskBackedPartitionStore partitionStore =
          (DiskBackedPartitionStore)
              oocEngine.getServerData().getPartitionStore();
      numBytesTransferred +=
          partitionStore.loadPartitionData(partitionId);
      if (currentSuperstep == BspService.INPUT_SUPERSTEP &&
          superstep == currentSuperstep) {
        DiskBackedEdgeStore edgeStore =
            (DiskBackedEdgeStore) oocEngine.getServerData().getEdgeStore();
        numBytesTransferred +=
            edgeStore.loadPartitionData(partitionId);
      }
      MessageStore messageStore;
      if (currentSuperstep == superstep) {
        messageStore = oocEngine.getServerData().getCurrentMessageStore();
      } else {
        Preconditions.checkState(superstep == currentSuperstep + 1);
        messageStore = oocEngine.getServerData().getIncomingMessageStore();
      }
      if (messageStore != null) {
        numBytesTransferred += ((DiskBackedMessageStore) messageStore)
            .loadPartitionData(partitionId);
      }
      oocEngine.getMetaPartitionManager()
          .doneLoadingPartition(partitionId, superstep);
      executed = true;
    }
    return executed;
  }

  @Override
  public IOCommandType getType() {
    return IOCommandType.LOAD_PARTITION;
  }

  @Override
  public String toString() {
    return "LoadPartitionIOCommand: (partitionId = " + partitionId + ", " +
        "superstep = " + superstep + ")";
  }
}
