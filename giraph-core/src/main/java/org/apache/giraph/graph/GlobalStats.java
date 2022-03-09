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

package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.partition.PartitionStats;
import org.apache.hadoop.io.Writable;

/**
 * Aggregated stats by the master.
 */
public class GlobalStats implements Writable {
  /** All vertices in the application */
  private long vertexCount = 0;
  /** All finished vertices in the last superstep */
  private long finishedVertexCount = 0;
  /** All edges in the last superstep */
  private long edgeCount = 0;
  /** All messages sent in the last superstep */
  private long messageCount = 0;
  /** All message bytes sent in the last superstep */
  private long messageBytesCount = 0;
  /** Whether the computation should be halted */
  private boolean haltComputation = false;
  /** Bytes of data stored to disk in the last superstep */
  private long oocStoreBytesCount = 0;
  /** Bytes of data loaded to disk in the last superstep */
  private long oocLoadBytesCount = 0;
  /** Lowest percentage of graph in memory throughout the execution */
  private int lowestGraphPercentageInMemory = 100;
  /**
   * Master's decision on whether we should checkpoint and
   * what to do next.
   */
  private CheckpointStatus checkpointStatus =
      CheckpointStatus.NONE;

  /**
   * Add the stats of a partition to the global stats.
   *
   * @param partitionStats Partition stats to be added.
   */
  public void addPartitionStats(PartitionStats partitionStats) {
    this.vertexCount += partitionStats.getVertexCount();
    this.finishedVertexCount += partitionStats.getFinishedVertexCount();
    this.edgeCount += partitionStats.getEdgeCount();
  }

  public long getVertexCount() {
    return vertexCount;
  }

  public long getFinishedVertexCount() {
    return finishedVertexCount;
  }

  public long getEdgeCount() {
    return edgeCount;
  }

  public long getMessageCount() {
    return messageCount;
  }

  public long getMessageBytesCount() {
    return messageBytesCount;
  }

  public boolean getHaltComputation() {
    return haltComputation;
  }

  public void setHaltComputation(boolean value) {
    haltComputation = value;
  }

  public long getOocStoreBytesCount() {
    return oocStoreBytesCount;
  }

  public long getOocLoadBytesCount() {
    return oocLoadBytesCount;
  }

  public CheckpointStatus getCheckpointStatus() {
    return checkpointStatus;
  }

  public void setCheckpointStatus(CheckpointStatus checkpointStatus) {
    this.checkpointStatus = checkpointStatus;
  }

  public int getLowestGraphPercentageInMemory() {
    return lowestGraphPercentageInMemory;
  }

  public void setLowestGraphPercentageInMemory(
      int lowestGraphPercentageInMemory) {
    this.lowestGraphPercentageInMemory = lowestGraphPercentageInMemory;
  }

  /**
   * Add bytes loaded to the global stats.
   *
   * @param oocLoadBytesCount number of bytes to be added
   */
  public void addOocLoadBytesCount(long oocLoadBytesCount) {
    this.oocLoadBytesCount += oocLoadBytesCount;
  }

  /**
   * Add bytes stored to the global stats.
   *
   * @param oocStoreBytesCount number of bytes to be added
   */
  public void addOocStoreBytesCount(long oocStoreBytesCount) {
    this.oocStoreBytesCount += oocStoreBytesCount;
  }

  /**
   * Add messages to the global stats.
   *
   * @param messageCount Number of messages to be added.
   */
  public void addMessageCount(long messageCount) {
    this.messageCount += messageCount;
  }

  /**
   * Add messages to the global stats.
   *
   * @param msgBytesCount Number of message bytes to be added.
   */
  public void addMessageBytesCount(long msgBytesCount) {
    this.messageBytesCount += msgBytesCount;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    vertexCount = input.readLong();
    finishedVertexCount = input.readLong();
    edgeCount = input.readLong();
    messageCount = input.readLong();
    messageBytesCount = input.readLong();
    oocLoadBytesCount = input.readLong();
    oocStoreBytesCount = input.readLong();
    lowestGraphPercentageInMemory = input.readInt();
    haltComputation = input.readBoolean();
    if (input.readBoolean()) {
      checkpointStatus = CheckpointStatus.values()[input.readInt()];
    } else {
      checkpointStatus = null;
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(vertexCount);
    output.writeLong(finishedVertexCount);
    output.writeLong(edgeCount);
    output.writeLong(messageCount);
    output.writeLong(messageBytesCount);
    output.writeLong(oocLoadBytesCount);
    output.writeLong(oocStoreBytesCount);
    output.writeInt(lowestGraphPercentageInMemory);
    output.writeBoolean(haltComputation);
    output.writeBoolean(checkpointStatus != null);
    if (checkpointStatus != null) {
      output.writeInt(checkpointStatus.ordinal());
    }
  }

  @Override
  public String toString() {
    return "(vtx=" + vertexCount + ",finVtx=" +
        finishedVertexCount + ",edges=" + edgeCount + ",msgCount=" +
        messageCount + ",msgBytesCount=" +
          messageBytesCount + ",haltComputation=" + haltComputation +
        ", checkpointStatus=" + checkpointStatus + ')';
  }
}
