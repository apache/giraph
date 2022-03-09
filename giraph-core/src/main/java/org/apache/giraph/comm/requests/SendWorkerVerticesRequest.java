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

package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Send to a worker one or more partitions of vertices
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class SendWorkerVerticesRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendWorkerVerticesRequest.class);
  /** Worker partitions to be sent */
  private PairList<Integer, ExtendedDataOutput> workerPartitions;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerVerticesRequest() { }

  /**
   * Constructor for sending a request.
   *
   * @param conf Configuration
   * @param workerPartitions Partitions to be send in this request
   */
  public SendWorkerVerticesRequest(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      PairList<Integer, ExtendedDataOutput> workerPartitions) {
    this.workerPartitions = workerPartitions;
    setConf(conf);
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    workerPartitions = new PairList<Integer, ExtendedDataOutput>();
    workerPartitions.initialize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      ExtendedDataOutput partitionData =
          WritableUtils.readExtendedDataOutput(input, getConf());
      workerPartitions.add(partitionId, partitionData);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(workerPartitions.getSize());
    PairList<Integer, ExtendedDataOutput>.Iterator
        iterator = workerPartitions.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      output.writeInt(iterator.getCurrentFirst());
      WritableUtils.writeExtendedDataOutput(
          iterator.getCurrentSecond(), output);
    }
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_VERTICES_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    PairList<Integer, ExtendedDataOutput>.Iterator
        iterator = workerPartitions.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      serverData.getPartitionStore()
          .addPartitionVertices(iterator.getCurrentFirst(),
              iterator.getCurrentSecond());
    }
  }

  @Override
  public int getSerializedSize() {
    // 4 for number of partitions
    int size = super.getSerializedSize() + 4;
    PairList<Integer, ExtendedDataOutput>.Iterator iterator =
        workerPartitions.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      // 4 bytes for the partition id and 4 bytes for the size
      size += 8 + iterator.getCurrentSecond().getPos();
    }
    return size;
  }
}

