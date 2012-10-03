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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.giraph.comm.ServerData;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Send a collection of vertex messages for a partition. It adds messages to
 * current message store and it should be used only during partition exchange.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SendPartitionCurrentMessagesRequest<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable> extends
  WritableRequest<I, V, E, M> implements WorkerRequest<I, V, E, M> {
  /** the destination partition for these vertices' messages*/
  private int partitionId;
  /** map of destination vertex ID's to message lists */
  private Map<I, Collection<M>> vertexMessageMap;

  /** Constructor used for reflection only */
  public SendPartitionCurrentMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partitionId Partition to send the request to
   * @param vertexIdMessages Map of messages to send
   */
  public SendPartitionCurrentMessagesRequest(int partitionId,
    Map<I, Collection<M>> vertexIdMessages) {
    super();
    this.partitionId = partitionId;
    this.vertexMessageMap = vertexIdMessages;
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_PARTITION_CURRENT_MESSAGES_REQUEST;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    partitionId = input.readInt();
    final int numVertices = input.readInt();
    vertexMessageMap =
      Maps.<I, Collection<M>>newHashMapWithExpectedSize(numVertices);
    for (int i = 0; i < numVertices; ++i) {
      I nextVertex = getConf().createVertexId();
      nextVertex.readFields(input);
      final int numMessages = input.readInt();
      Collection<M> messagesForVertex =
        Lists.<M>newArrayListWithExpectedSize(numMessages);
      vertexMessageMap.put(nextVertex, messagesForVertex);
      for (int j = 0; j < numMessages; ++j) {
        M nextMessage = getConf().createMessageValue();
        nextMessage.readFields(input);
        messagesForVertex.add(nextMessage);
      }
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    output.writeInt(vertexMessageMap.size());
    for (Entry<I, Collection<M>> entry : vertexMessageMap.entrySet()) {
      entry.getKey().write(output);
      output.writeInt(entry.getValue().size());
      for (M message : entry.getValue()) {
        message.write(output);
      }
    }
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    try {
      serverData.getCurrentMessageStore().addPartitionMessages(
        vertexMessageMap, partitionId);
    } catch (IOException e) {
      throw new RuntimeException("doRequest: Got IOException ", e);
    }
  }
}
