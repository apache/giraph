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

package org.apache.giraph.comm.messages;

import org.apache.giraph.comm.RequestRegistry.Type;
import org.apache.giraph.comm.SendPartitionMessagesRequest;
import org.apache.giraph.comm.ServerData;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

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
    SendPartitionMessagesRequest<I, V, E, M> {
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
    super(partitionId, vertexIdMessages);
  }

  @Override
  public Type getType() {
    return Type.SEND_PARTITION_CURRENT_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    try {
      serverData.getCurrentMessageStore().addPartitionMessages(
          getVertexIdMessages(), getPartitionId());
    } catch (IOException e) {
      throw new RuntimeException("doRequest: Got IOException ", e);
    }
  }
}
