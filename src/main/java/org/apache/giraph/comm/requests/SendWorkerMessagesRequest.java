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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
  private Map<Integer, Map<I, Collection<M>>> partitionVertexMessagesMap;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgsMap Map of remote partitions => vertices => messages
   */
  public SendWorkerMessagesRequest(
    Map<Integer, Map<I, Collection<M>>> partVertMsgsMap) {
    super();
    this.partitionVertexMessagesMap = partVertMsgsMap;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    partitionVertexMessagesMap = Maps.<Integer, Map<I, Collection<M>>>
      newHashMapWithExpectedSize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      int numVertices = input.readInt();
      Map<I, Collection<M>> vertexIdMessages =
        Maps.<I, Collection<M>>newHashMapWithExpectedSize(numVertices);
      partitionVertexMessagesMap.put(partitionId, vertexIdMessages);
      while (numVertices-- > 0) {
        I vertexId = getConf().createVertexId();
        vertexId.readFields(input);
        int messageCount = input.readInt();
        List<M> messageList =
          Lists.newArrayListWithExpectedSize(messageCount);
        while (messageCount-- > 0) {
          M message = getConf().createMessageValue();
          message.readFields(input);
          messageList.add(message);
        }
        if (vertexIdMessages.put(vertexId, messageList) != null) {
          throw new IllegalStateException(
            "readFields: Already has vertex id " + vertexId);
        }
      }
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionVertexMessagesMap.size());
    for (Entry<Integer, Map<I, Collection<M>>> partitionEntry :
      partitionVertexMessagesMap.entrySet()) {
      output.writeInt(partitionEntry.getKey());
      output.writeInt(partitionEntry.getValue().size());
      for (Entry<I, Collection<M>> vertexEntry :
        partitionEntry.getValue().entrySet()) {
        vertexEntry.getKey().write(output);
        output.writeInt(vertexEntry.getValue().size());
        for (M message : vertexEntry.getValue()) {
          message.write(output);
        }
      }
    }
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    for (Entry<Integer, Map<I, Collection<M>>> entry :
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
