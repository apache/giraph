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
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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
  WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {
  /** Destination partition for these vertices' messages*/
  private int partitionId;
  /** Map of destination vertex ID's to message lists */
  private ByteArrayVertexIdMessages<I, M> vertexIdMessageMap;

  /** Constructor used for reflection only */
  public SendPartitionCurrentMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partitionId Partition to send the request to
   * @param vertexIdMessages Map of messages to send
   */
  public SendPartitionCurrentMessagesRequest(int partitionId,
    ByteArrayVertexIdMessages<I, M> vertexIdMessages) {
    super();
    this.partitionId = partitionId;
    this.vertexIdMessageMap = vertexIdMessages;
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_PARTITION_CURRENT_MESSAGES_REQUEST;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    partitionId = input.readInt();
    // At this moment the Computation class have already been replaced with
    // the new one, and we deal with messages from previous superstep
    vertexIdMessageMap = new ByteArrayVertexIdMessages<>(
        getConf().<M>createIncomingMessageValueFactory());
    vertexIdMessageMap.setConf(getConf());
    vertexIdMessageMap.initialize();
    vertexIdMessageMap.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    vertexIdMessageMap.write(output);
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    serverData.<M>getCurrentMessageStore().addPartitionMessages(partitionId,
        vertexIdMessageMap);
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() + 4 +
        vertexIdMessageMap.getSerializedSize();
  }
}
