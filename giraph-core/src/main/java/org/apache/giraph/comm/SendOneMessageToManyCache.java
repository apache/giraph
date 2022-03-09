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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.SendWorkerOneMessageToManyRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayOneMessageToManyIds;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Aggregates the messages to be sent to workers so they can be sent
 * in bulk.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public class SendOneMessageToManyCache<I extends WritableComparable,
  M extends Writable> extends SendMessageCache<I, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendOneMessageToManyCache.class);
  /** Cache serialized one to many messages for each worker */
  private final ByteArrayOneMessageToManyIds<I, M>[] msgVidsCache;
  /** Tracking message-vertexIds sizes for each worker */
  private final int[] msgVidsSizes;
  /** Reused byte array to serialize target ids on each worker */
  private final ExtendedDataOutput[] idSerializer;
  /** Reused int array to count target id distribution */
  private final int[] idCounter;
  /**
   * Reused int array to record the partition id
   * of the first target vertex id found on the worker.
   */
  private final int[] firstPartitionMap;
  /** The WorkerInfo list */
  private final WorkerInfo[] workerInfoList;

  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param processor NettyWorkerClientRequestProcessor
   * @param maxMsgSize Max message size sent to a worker
   */
  public SendOneMessageToManyCache(ImmutableClassesGiraphConfiguration conf,
    CentralizedServiceWorker<?, ?, ?> serviceWorker,
    NettyWorkerClientRequestProcessor<I, ?, ?> processor,
    int maxMsgSize) {
    super(conf, serviceWorker, processor, maxMsgSize);
    int numWorkers = getNumWorkers();
    msgVidsCache = new ByteArrayOneMessageToManyIds[numWorkers];
    msgVidsSizes = new int[numWorkers];
    idSerializer = new ExtendedDataOutput[numWorkers];
    // InitialBufferSizes is alo initialized based on the number of workers.
    // As a result, initialBufferSizes is the same as idSerializer in length
    int initialBufferSize = 0;
    for (int i = 0; i < this.idSerializer.length; i++) {
      initialBufferSize = getSendWorkerInitialBufferSize(i);
      if (initialBufferSize > 0) {
        // InitialBufferSizes is from super class.
        // Each element is for one worker.
        idSerializer[i] = conf.createExtendedDataOutput(initialBufferSize);
      }
    }
    idCounter = new int[numWorkers];
    firstPartitionMap = new int[numWorkers];
    // Get worker info list.
    workerInfoList = new WorkerInfo[numWorkers];
    // Remember there could be null in the array.
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      workerInfoList[workerInfo.getTaskId()] = workerInfo;
    }
  }

  /**
   * Reset ExtendedDataOutput array for id serialization
   * in next message-Vids encoding
   */
  private void resetIdSerializers() {
    for (int i = 0; i < this.idSerializer.length; i++) {
      if (idSerializer[i] != null) {
        idSerializer[i].reset();
      }
    }
  }

  /**
   * Reset id counter for next message-vertexIds encoding
   */
  private void resetIdCounter() {
    Arrays.fill(idCounter, 0);
  }

  /**
   * Add message with multiple target ids to message cache.
   *
   * @param workerInfo The remote worker destination
   * @param ids A byte array to hold serialized vertex ids
   * @param idPos The end position of ids
   *              information in the byte array above
   * @param count The number of target ids
   * @param message Message to send to remote worker
   * @return The size of messages for the worker.
   */
  private int addOneToManyMessage(
    WorkerInfo workerInfo, byte[] ids, int idPos, int count, M message) {
    // Get the data collection
    ByteArrayOneMessageToManyIds<I, M> workerData =
      msgVidsCache[workerInfo.getTaskId()];
    if (workerData == null) {
      workerData = new ByteArrayOneMessageToManyIds<I, M>(
          messageValueFactory);
      workerData.setConf(getConf());
      workerData.initialize(getSendWorkerInitialBufferSize(
        workerInfo.getTaskId()));
      msgVidsCache[workerInfo.getTaskId()] = workerData;
    }
    workerData.add(ids, idPos, count, message);
    // Update the size of cached, outgoing data per worker
    msgVidsSizes[workerInfo.getTaskId()] =
      workerData.getSize();
    return msgVidsSizes[workerInfo.getTaskId()];
  }

  /**
   * Gets the messages + vertexIds for a worker and removes it from the cache.
   * Here the {@link org.apache.giraph.utils.ByteArrayOneMessageToManyIds}
   * returned could be null.But when invoking this method, we also check if
   * the data size sent to this worker is above the threshold.
   * Therefore, it doesn't matter if the result is null or not.
   *
   * @param workerInfo Target worker to which one messages - many ids are sent
   * @return {@link org.apache.giraph.utils.ByteArrayOneMessageToManyIds}
   *         that belong to the workerInfo
   */
  private ByteArrayOneMessageToManyIds<I, M>
  removeWorkerMsgVids(WorkerInfo workerInfo) {
    ByteArrayOneMessageToManyIds<I, M> workerData =
      msgVidsCache[workerInfo.getTaskId()];
    if (workerData != null) {
      msgVidsCache[workerInfo.getTaskId()] = null;
      msgVidsSizes[workerInfo.getTaskId()] = 0;
    }
    return workerData;
  }

  /**
   * Gets all messages - vertexIds and removes them from the cache.
   *
   * @return All vertex messages for all workers
   */
  private PairList<WorkerInfo, ByteArrayOneMessageToManyIds<I, M>>
  removeAllMsgVids() {
    PairList<WorkerInfo, ByteArrayOneMessageToManyIds<I, M>> allData =
      new PairList<WorkerInfo, ByteArrayOneMessageToManyIds<I, M>>();
    allData.initialize(msgVidsCache.length);
    for (WorkerInfo workerInfo : getWorkerPartitions().keySet()) {
      ByteArrayOneMessageToManyIds<I, M> workerData =
        removeWorkerMsgVids(workerInfo);
      if (workerData != null && !workerData.isEmpty()) {
        allData.add(workerInfo, workerData);
      }
    }
    return allData;
  }

  @Override
  public void sendMessageToAllRequest(Iterator<I> vertexIdIterator, M message) {
    // This is going to be reused through every message sending
    resetIdSerializers();
    resetIdCounter();
    // Count messages
    int currentMachineId = 0;
    PartitionOwner owner = null;
    WorkerInfo workerInfo = null;
    I vertexId = null;
    while (vertexIdIterator.hasNext()) {
      vertexId = vertexIdIterator.next();
      owner = getServiceWorker().getVertexPartitionOwner(vertexId);
      workerInfo = owner.getWorkerInfo();
      currentMachineId = workerInfo.getTaskId();
      // Serialize this target vertex id
      try {
        vertexId.write(idSerializer[currentMachineId]);
      } catch (IOException e) {
        throw new IllegalStateException(
          "Failed to serialize the target vertex id.");
      }
      idCounter[currentMachineId]++;
      // Record the first partition id in the worker which message send to.
      // If idCounter shows there is only one target on this worker
      // then this is the partition number of the target vertex.
      if (idCounter[currentMachineId] == 1) {
        firstPartitionMap[currentMachineId] = owner.getPartitionId();
      }
    }
    // Add the message to the cache
    int idSerializerPos = 0;
    int workerMessageSize = 0;
    byte[] serializedId  = null;
    WritableRequest writableRequest = null;
    for (int i = 0; i < idCounter.length; i++) {
      if (idCounter[i] == 1) {
        serializedId = idSerializer[i].getByteArray();
        idSerializerPos = idSerializer[i].getPos();
        // Add the message to the cache
        workerMessageSize = addMessage(workerInfoList[i],
          firstPartitionMap[i], serializedId, idSerializerPos, message);

        if (LOG.isTraceEnabled()) {
          LOG.trace("sendMessageToAllRequest: Send bytes (" +
            message.toString() + ") to one target in  worker " +
            workerInfoList[i]);
        }
        ++totalMsgsSentInSuperstep;
        if (workerMessageSize >= maxMessagesSizePerWorker) {
          PairList<Integer, VertexIdMessages<I, M>>
            workerMessages = removeWorkerMessages(workerInfoList[i]);
          writableRequest = new SendWorkerMessagesRequest<>(workerMessages);
          totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
          clientProcessor.doRequest(workerInfoList[i], writableRequest);
          // Notify sending
          getServiceWorker().getGraphTaskManager().notifySentMessages();
        }
      } else if (idCounter[i] > 1) {
        serializedId = idSerializer[i].getByteArray();
        idSerializerPos = idSerializer[i].getPos();
        workerMessageSize = addOneToManyMessage(
            workerInfoList[i], serializedId, idSerializerPos, idCounter[i],
            message);

        if (LOG.isTraceEnabled()) {
          LOG.trace("sendMessageToAllRequest: Send bytes (" +
            message.toString() + ") to all targets in worker" +
            workerInfoList[i]);
        }
        totalMsgsSentInSuperstep += idCounter[i];
        if (workerMessageSize >= maxMessagesSizePerWorker) {
          ByteArrayOneMessageToManyIds<I, M> workerMsgVids =
            removeWorkerMsgVids(workerInfoList[i]);
          writableRequest =  new SendWorkerOneMessageToManyRequest<>(
            workerMsgVids, getConf());
          totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
          clientProcessor.doRequest(workerInfoList[i], writableRequest);
          // Notify sending
          getServiceWorker().getGraphTaskManager().notifySentMessages();
        }
      }
    }
  }

  @Override
  public void flush() {
    super.flush();
    PairList<WorkerInfo, ByteArrayOneMessageToManyIds<I, M>>
    remainingMsgVidsCache = removeAllMsgVids();
    PairList<WorkerInfo,
        ByteArrayOneMessageToManyIds<I, M>>.Iterator
    msgIdsIterator = remainingMsgVidsCache.getIterator();
    while (msgIdsIterator.hasNext()) {
      msgIdsIterator.next();
      WritableRequest writableRequest =
        new SendWorkerOneMessageToManyRequest<>(
            msgIdsIterator.getCurrentSecond(), getConf());
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(
        msgIdsIterator.getCurrentFirst(), writableRequest);
    }
  }
}
