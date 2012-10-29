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
import org.apache.giraph.comm.VertexIdMessageCollection;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

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
  private Map<Integer, VertexIdMessageCollection<I, M>>
  partitionVertexMessagesMap;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgsMap Map of remote partitions =>
   *                        VertexIdMessageCollection
   */
  public SendWorkerMessagesRequest(
    Map<Integer, VertexIdMessageCollection<I, M>> partVertMsgsMap) {
    super();
    this.partitionVertexMessagesMap = partVertMsgsMap;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    partitionVertexMessagesMap =
        Maps.<Integer, VertexIdMessageCollection<I, M>>
            newHashMapWithExpectedSize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      VertexIdMessageCollection<I, M> vertexIdMessages =
          new VertexIdMessageCollection<I, M>(getConf());
      vertexIdMessages.readFields(input);
      partitionVertexMessagesMap.put(partitionId, vertexIdMessages);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionVertexMessagesMap.size());
    for (Entry<Integer, VertexIdMessageCollection<I, M>> partitionEntry :
      partitionVertexMessagesMap.entrySet()) {
      output.writeInt(partitionEntry.getKey());
      partitionEntry.getValue().write(output);
    }
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    for (Entry<Integer, VertexIdMessageCollection<I, M>> entry :
      partitionVertexMessagesMap.entrySet()) {
      try {
        serverData.getIncomingMessageStore()
          .addPartitionMessages(entry.getValue(), entry.getKey());
      } catch (IOException e) {
        throw new RuntimeException("doRequest: Got IOException ", e);
      }
    }
  }
}
