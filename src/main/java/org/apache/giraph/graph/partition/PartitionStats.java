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

package org.apache.giraph.graph.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Used to keep track of statistics of every {@link Partition}. Contains no
 * actual partition data, only the statistics.
 */
public class PartitionStats implements Writable {
    private int partitionId = -1;
    private long vertexCount = 0;
    private long finishedVertexCount = 0;
    private long edgeCount = 0;

    public PartitionStats() {}

    public PartitionStats(int partitionId,
                          long vertexCount,
                          long finishedVertexCount,
                          long edgeCount) {
        this.partitionId = partitionId;
        this.vertexCount = vertexCount;
        this.finishedVertexCount = finishedVertexCount;
        this.edgeCount = edgeCount;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void incrVertexCount() {
        ++vertexCount;
    }

    public long getVertexCount() {
        return vertexCount;
    }

    public void incrFinishedVertexCount() {
        ++finishedVertexCount;
    }

    public long getFinishedVertexCount() {
        return finishedVertexCount;
    }

    public void addEdgeCount(long edgeCount) {
        this.edgeCount += edgeCount;
    }

    public long getEdgeCount() {
        return edgeCount;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        partitionId = input.readInt();
        vertexCount = input.readLong();
        finishedVertexCount = input.readLong();
        edgeCount = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(partitionId);
        output.writeLong(vertexCount);
        output.writeLong(finishedVertexCount);
        output.writeLong(edgeCount);
    }

    @Override
    public String toString() {
        return "(id=" + partitionId + ",vtx=" + vertexCount + ",finVtx=" +
               finishedVertexCount + ",edges=" + edgeCount + ")";
    }
}
