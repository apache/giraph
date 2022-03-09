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

import org.apache.giraph.utils.VertexIdData;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract request to send a collection of data, indexed by vertex id,
 * for a partition.
 *
 * @param <I> Vertex id
 * @param <T> Data
 * @param <B> Specialization of
 * {@link org.apache.giraph.utils.VertexIdData} for T
 */
@SuppressWarnings("unchecked")
public abstract class SendWorkerDataRequest<I extends WritableComparable, T,
    B extends VertexIdData<I, T>>
    extends WritableRequest implements WorkerRequest {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendWorkerDataRequest.class);
  /**
   * All data for a group of vertices, organized by partition, which
   * are owned by a single (destination) worker. This data is all
   * destined for this worker.
   * */
  protected PairList<Integer, B> partitionVertexData;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerDataRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertData Map of remote partitions =&gt; VertexIdData
   */
  public SendWorkerDataRequest(
      PairList<Integer, B> partVertData) {
    this.partitionVertexData = partVertData;
  }

  /**
   * Create a new {@link org.apache.giraph.utils.VertexIdData}
   * specialized for the use case.
   *
   * @return A new instance of
   * {@link org.apache.giraph.utils.VertexIdData}
   */
  public abstract B createVertexIdData();

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    partitionVertexData = new PairList<Integer, B>();
    partitionVertexData.initialize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      B vertexIdData = createVertexIdData();
      vertexIdData.setConf(getConf());
      vertexIdData.readFields(input);
      partitionVertexData.add(partitionId, vertexIdData);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionVertexData.getSize());
    PairList<Integer, B>.Iterator
        iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      output.writeInt(iterator.getCurrentFirst());
      iterator.getCurrentSecond().write(output);
    }
  }

  @Override
  public int getSerializedSize() {
    int size = super.getSerializedSize() + 4;
    PairList<Integer, B>.Iterator iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      size += 4 + iterator.getCurrentSecond().getSerializedSize();
    }
    return size;
  }
}

