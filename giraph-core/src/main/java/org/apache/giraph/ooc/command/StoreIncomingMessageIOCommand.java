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
import org.apache.giraph.ooc.data.DiskBackedMessageStore;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

/**
 * IOCommand to store incoming message of a particular partition.
 */
public class StoreIncomingMessageIOCommand extends IOCommand {
  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param partitionId id of the partition to store its incoming messages
   */
  public StoreIncomingMessageIOCommand(OutOfCoreEngine oocEngine,
                                       int partitionId) {
    super(oocEngine, partitionId);
  }

  @Override
  public boolean execute() throws IOException {
    boolean executed = false;
    if (oocEngine.getMetaPartitionManager()
        .startOffloadingMessages(partitionId)) {
      DiskBackedMessageStore messageStore =
          (DiskBackedMessageStore)
              oocEngine.getServerData().getIncomingMessageStore();
      checkState(messageStore != null);
      numBytesTransferred +=
          messageStore.offloadPartitionData(partitionId);
      oocEngine.getMetaPartitionManager().doneOffloadingMessages(partitionId);
      executed = true;
    }
    return executed;
  }

  @Override
  public IOCommandType getType() {
    return IOCommandType.STORE_MESSAGE;
  }

  @Override
  public String toString() {
    return "StoreIncomingMessageIOCommand: (partitionId = " + partitionId + ")";
  }
}
