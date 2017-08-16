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

package org.apache.giraph.comm;

import static org.apache.giraph.conf.GiraphConstants.ADDITIONAL_MSG_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_MSG_REQUEST_SIZE;

import java.util.Iterator;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Aggregates the messages to be sent to workers so they can be sent
 * in bulk.  Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendMessageCache<I extends WritableComparable, M extends Writable>
    extends SendVertexIdDataCache<I, M, VertexIdMessages<I, M>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendMessageCache.class);
  /** Messages sent during the last superstep */
  protected long totalMsgsSentInSuperstep = 0;
  /** Message bytes sent during the last superstep */
  protected long totalMsgBytesSentInSuperstep = 0;
  /** Max message size sent to a worker */
  protected final int maxMessagesSizePerWorker;
  /** NettyWorkerClientRequestProcessor for message sending */
  protected final NettyWorkerClientRequestProcessor<I, ?, ?> clientProcessor;
  /** Cached message value factory */
  protected MessageValueFactory<M> messageValueFactory;
  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param processor NettyWorkerClientRequestProcessor
   * @param maxMsgSize Max message size sent to a worker
   */
  public SendMessageCache(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?> serviceWorker,
      NettyWorkerClientRequestProcessor<I, ?, ?> processor,
      int maxMsgSize) {
    super(conf, serviceWorker, MAX_MSG_REQUEST_SIZE.get(conf),
        ADDITIONAL_MSG_REQUEST_SIZE.get(conf));
    maxMessagesSizePerWorker = maxMsgSize;
    clientProcessor = processor;
    messageValueFactory =
            conf.createOutgoingMessageValueFactory();
  }

  @Override
  public VertexIdMessages<I, M> createVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>(messageValueFactory);
  }

  /**
   * Add a message to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param message Message to send to remote worker
   * @return Size of messages for the worker.
   */
  public int addMessage(WorkerInfo workerInfo,
                        int partitionId, I destVertexId, M message) {
    return addData(workerInfo, partitionId, destVertexId, message);
  }

  /**
   * Add a message to the cache with serialized ids.
   *
   * @param workerInfo The remote worker destination
   * @param partitionId The remote Partition this message belongs to
   * @param serializedId Serialized vertex id that is ultimate destination
   * @param idSerializerPos The end position of serialized id in the byte array
   * @param message Message to send to remote worker
   * @return Size of messages for the worker.
   */
  protected int addMessage(WorkerInfo workerInfo, int partitionId,
      byte[] serializedId, int idSerializerPos, M message) {
    return addData(
      workerInfo, partitionId, serializedId,
      idSerializerPos, message);
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return List of pairs (partitionId, ByteArrayVertexIdMessages),
   *         where all partition ids belong to workerInfo
   */
  protected PairList<Integer, VertexIdMessages<I, M>>
  removeWorkerMessages(WorkerInfo workerInfo) {
    return removeWorkerData(workerInfo);
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  private PairList<WorkerInfo, PairList<
      Integer, VertexIdMessages<I, M>>> removeAllMessages() {
    return removeAllData();
  }

  /**
   * Send a message to a target vertex id.
   *
   * @param destVertexId Target vertex id
   * @param message The message sent to the target
   */
  public void sendMessageRequest(I destVertexId, M message) {
    PartitionOwner owner =
      getServiceWorker().getVertexPartitionOwner(destVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendMessageRequest: Send bytes (" + message.toString() +
        ") to " + destVertexId + " on worker " + workerInfo);
    }
    ++totalMsgsSentInSuperstep;
    // Add the message to the cache
    int workerMessageSize = addMessage(
      workerInfo, partitionId, destVertexId, message);
    // Send a request if the cache of outgoing message to
    // the remote worker 'workerInfo' is full enough to be flushed
    if (workerMessageSize >= maxMessagesSizePerWorker) {
      PairList<Integer, VertexIdMessages<I, M>>
        workerMessages = removeWorkerMessages(workerInfo);
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, M>(workerMessages);
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(workerInfo, writableRequest);
      // Notify sending
      getServiceWorker().getGraphTaskManager().notifySentMessages();
    }
  }

  /**
   * An iterator wrapper on edges to return
   * target vertex ids.
   */
  public static class TargetVertexIdIterator<I extends WritableComparable>
      implements Iterator<I> {
    /** An edge iterator */
    private final Iterator<Edge<I, Writable>> edgesIterator;

    /**
     * Constructor.
     *
     * @param vertex The source vertex of the out edges
     */
    public TargetVertexIdIterator(Vertex<I, ?, ?> vertex) {
      edgesIterator =
        ((Vertex<I, Writable, Writable>) vertex).getEdges().iterator();
    }

    @Override
    public boolean hasNext() {
      return edgesIterator.hasNext();
    }

    @Override
    public I next() {
      return edgesIterator.next().getTargetVertexId();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Send message to all its neighbors
   *
   * @param vertex The source vertex
   * @param message The message sent to a worker
   */
  public void sendMessageToAllRequest(Vertex<I, ?, ?> vertex, M message) {
    TargetVertexIdIterator targetVertexIterator =
      new TargetVertexIdIterator(vertex);
    sendMessageToAllRequest(targetVertexIterator, message);
  }

  /**
   * Send message to the target ids in the iterator
   *
   * @param vertexIdIterator The iterator of target vertex ids
   * @param message The message sent to a worker
   */
  public void sendMessageToAllRequest(Iterator<I> vertexIdIterator, M message) {
    while (vertexIdIterator.hasNext()) {
      sendMessageRequest(vertexIdIterator.next(), message);
    }
  }

  /**
   * Flush the rest of the messages to the workers.
   */
  public void flush() {
    PairList<WorkerInfo, PairList<Integer,
        VertexIdMessages<I, M>>>
    remainingMessageCache = removeAllMessages();
    PairList<WorkerInfo, PairList<
        Integer, VertexIdMessages<I, M>>>.Iterator
    iterator = remainingMessageCache.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, M>(
          iterator.getCurrentSecond());
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(
        iterator.getCurrentFirst(), writableRequest);
    }
  }

  /**
   * Reset the message count per superstep.
   *
   * @return The message count sent in last superstep
   */
  public long resetMessageCount() {
    long messagesSentInSuperstep = totalMsgsSentInSuperstep;
    totalMsgsSentInSuperstep = 0;
    return messagesSentInSuperstep;
  }

  /**
   * Reset the message bytes count per superstep.
   *
   * @return The message count sent in last superstep
   */
  public long resetMessageBytesCount() {
    long messageBytesSentInSuperstep = totalMsgBytesSentInSuperstep;
    totalMsgBytesSentInSuperstep = 0;
    return messageBytesSentInSuperstep;
  }
}
