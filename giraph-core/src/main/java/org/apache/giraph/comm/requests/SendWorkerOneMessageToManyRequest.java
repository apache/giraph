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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayOneMessageToManyIds;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of ByteArrayOneMessageToManyIds messages to a worker.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerOneMessageToManyRequest<I extends WritableComparable,
    M extends Writable> extends WritableRequest<I, Writable, Writable>
    implements WorkerRequest<I, Writable, Writable> {
  /** ByteArrayOneMessageToManyIds encoding of vertexId &amp; messages */
  protected ByteArrayOneMessageToManyIds<I, M> oneMessageToManyIds;

  /**
   * Constructor used for reflection only.
   */
  public SendWorkerOneMessageToManyRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param oneMessageToManyIds ByteArrayOneMessageToManyIds
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendWorkerOneMessageToManyRequest(
      ByteArrayOneMessageToManyIds<I, M> oneMessageToManyIds,
      ImmutableClassesGiraphConfiguration conf) {
    this.oneMessageToManyIds = oneMessageToManyIds;
    setConf(conf);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_ONE_MESSAGE_TO_MANY_REQUEST;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    oneMessageToManyIds = new ByteArrayOneMessageToManyIds<>(
        getConf().<M>createOutgoingMessageValueFactory());
    oneMessageToManyIds.setConf(getConf());
    oneMessageToManyIds.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    this.oneMessageToManyIds.write(output);
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() +
        this.oneMessageToManyIds.getSerializedSize();
  }

  @Override
  public void doRequest(ServerData serverData) {
    MessageStore<I, M> messageStore = serverData.getIncomingMessageStore();
    if (messageStore.isPointerListEncoding()) {
      // if message store is pointer list based then send data as is
      messageStore.addPartitionMessages(-1, oneMessageToManyIds);
    } else { // else split the data per partition and send individually
      CentralizedServiceWorker<I, ?, ?> serviceWorker =
          serverData.getServiceWorker();
      // Get the initial size of ByteArrayVertexIdMessages per partition
      // on this worker. To make sure every ByteArrayVertexIdMessages to have
      // enough space to store the messages, we divide the original
      // ByteArrayOneMessageToManyIds message size by the number of partitions
      // and double the size
      // (Assume the major component in ByteArrayOneMessageToManyIds message
      // is a id list. Now each target id has a copy of message,
      // therefore we double the buffer size)
      // to get the initial size of ByteArrayVertexIdMessages.
      int initialSize = oneMessageToManyIds.getSize() /
          serverData.getPartitionStore().getNumPartitions() * 2;
      // Create ByteArrayVertexIdMessages for
      // message reformatting.
      Int2ObjectOpenHashMap<ByteArrayVertexIdMessages> partitionIdMsgs =
          new Int2ObjectOpenHashMap<>();

      // Put data from ByteArrayOneMessageToManyIds
      // to ByteArrayVertexIdMessages
      VertexIdMessageIterator<I, M> vertexIdMessageIterator =
        oneMessageToManyIds.getVertexIdMessageIterator();
      while (vertexIdMessageIterator.hasNext()) {
        vertexIdMessageIterator.next();
        M msg = vertexIdMessageIterator.getCurrentMessage();
        I vertexId = vertexIdMessageIterator.getCurrentVertexId();
        PartitionOwner owner =
            serviceWorker.getVertexPartitionOwner(vertexId);
        int partitionId = owner.getPartitionId();
        ByteArrayVertexIdMessages<I, M> idMsgs = partitionIdMsgs
            .get(partitionId);
        if (idMsgs == null) {
          idMsgs = new ByteArrayVertexIdMessages<>(
              getConf().<M>createOutgoingMessageValueFactory());
          idMsgs.setConf(getConf());
          idMsgs.initialize(initialSize);
          partitionIdMsgs.put(partitionId, idMsgs);
        }
        idMsgs.add(vertexId, msg);
      }

      // Read ByteArrayVertexIdMessages and write to message store
      for (Entry<Integer, ByteArrayVertexIdMessages> idMsgs :
          partitionIdMsgs.entrySet()) {
        if (!idMsgs.getValue().isEmpty()) {
          serverData.getIncomingMessageStore().addPartitionMessages(
              idMsgs.getKey(), idMsgs.getValue());
        }
      }
    }
  }
}
