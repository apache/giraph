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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.utils.ByteArrayVertexIdMessageCollection;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Send a collection of vertex messages for a partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SendWorkerMessagesRequest<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> extends
    WritableRequest<I, V, E, M> implements WorkerRequest<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendWorkerMessagesRequest.class);
  /**
   * All messages for a group of vertices, organized by partition, which
   * are owned by a single (destination) worker. These messages are all
   * destined for this worker.
   * */
  private PairList<Integer, ByteArrayVertexIdMessageCollection<I, M>>
  partitionVertexMessages;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =>
   *                     ByteArrayVertexIdMessageCollection
   */
  public SendWorkerMessagesRequest(
    PairList<Integer, ByteArrayVertexIdMessageCollection<I, M>> partVertMsgs) {
    super();
    this.partitionVertexMessages = partVertMsgs;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    partitionVertexMessages =
        new PairList<Integer, ByteArrayVertexIdMessageCollection<I, M>>();
    partitionVertexMessages.initialize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      ByteArrayVertexIdMessageCollection<I, M> vertexIdMessages =
          new ByteArrayVertexIdMessageCollection<I, M>();
      vertexIdMessages.setConf(getConf());
      vertexIdMessages.readFields(input);
      partitionVertexMessages.add(partitionId, vertexIdMessages);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionVertexMessages.getSize());
    PairList<Integer, ByteArrayVertexIdMessageCollection<I, M>>.Iterator
        iterator = partitionVertexMessages.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      output.writeInt(iterator.getCurrentFirst());
      iterator.getCurrentSecond().write(output);
    }
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    PairList<Integer, ByteArrayVertexIdMessageCollection<I, M>>.Iterator
        iterator = partitionVertexMessages.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      try {
        serverData.getIncomingMessageStore()
            .addPartitionMessages(iterator.getCurrentSecond(),
                iterator.getCurrentFirst());
      } catch (IOException e) {
        throw new RuntimeException("doRequest: Got IOException ", e);
      }
    }
  }
}
