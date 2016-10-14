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
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of vertex messages for a partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerMessagesRequest<I extends WritableComparable,
    M extends Writable> extends SendWorkerDataRequest<I, M,
    VertexIdMessages<I, M>> {

  /** Default constructor */
  public SendWorkerMessagesRequest() {
  }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =&gt;
   *                     VertexIdMessages
   */
  public SendWorkerMessagesRequest(
      PairList<Integer, VertexIdMessages<I, M>> partVertMsgs) {
    this.partitionVertexData = partVertMsgs;
  }

  @Override
  public VertexIdMessages<I, M> createVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>(
        getConf().createOutgoingMessageValueFactory());
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData serverData) {
    PairList<Integer, VertexIdMessages<I, M>>.Iterator
        iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      serverData.getIncomingMessageStore().
          addPartitionMessages(iterator.getCurrentFirst(),
              iterator.getCurrentSecond());
    }
  }
}
