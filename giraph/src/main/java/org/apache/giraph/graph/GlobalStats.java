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

import org.apache.giraph.graph.partition.PartitionStats;
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
  /** Whether the computation should be halted */
  private boolean haltComputation = false;

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

  public boolean getHaltComputation() {
    return haltComputation;
  }

  public void setHaltComputation(boolean value) {
    haltComputation = value;
  }

  /**
   * Add messages to the global stats.
   *
   * @param messageCount Number of messages to be added.
   */
  public void addMessageCount(long messageCount) {
    this.messageCount += messageCount;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    vertexCount = input.readLong();
    finishedVertexCount = input.readLong();
    edgeCount = input.readLong();
    messageCount = input.readLong();
    haltComputation = input.readBoolean();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(vertexCount);
    output.writeLong(finishedVertexCount);
    output.writeLong(edgeCount);
    output.writeLong(messageCount);
    output.writeBoolean(haltComputation);
  }

  @Override
  public String toString() {
    return "(vtx=" + vertexCount + ",finVtx=" +
        finishedVertexCount + ",edges=" + edgeCount + ",msgCount=" +
        messageCount + ",haltComputation=" + haltComputation + ")";
  }
}
