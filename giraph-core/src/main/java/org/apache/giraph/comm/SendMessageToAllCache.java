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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.SendWorkerOneToAllMessagesRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayOneToAllMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.PairList;
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
public class SendMessageToAllCache<I extends WritableComparable,
    M extends Writable> extends SendMessageCache<I, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendMessageToAllCache.class);
  /** Cache serialized messages for each worker */
  private final ByteArrayOneToAllMessages<I, M>[] oneToAllMsgCache;
  /** Tracking one-to-all message sizes for each worker */
  private final int[] oneToAllMsgSizes;
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
  public SendMessageToAllCache(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?> serviceWorker,
      NettyWorkerClientRequestProcessor<I, ?, ?> processor,
      int maxMsgSize) {
    super(conf, serviceWorker, processor, maxMsgSize);
    int numWorkers = getNumWorkers();
    oneToAllMsgCache = new ByteArrayOneToAllMessages[numWorkers];
    oneToAllMsgSizes = new int[numWorkers];
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
   * in next "one-to-all" message sending.
   */
  private void resetIdSerializers() {
    for (int i = 0; i < this.idSerializer.length; i++) {
      if (idSerializer[i] != null) {
        idSerializer[i].reset();
      }
    }
  }

  /**
   * Reset id counter for next "one-to-all" message sending.
   */
  private void resetIdCounter() {
    Arrays.fill(idCounter, 0);
  }

  /**
   * Add message with multiple ids to
   * one-to-all message cache.
   *
   * @param workerInfo The remote worker destination
   * @param ids A byte array to hold serialized vertex ids
   * @param idPos The end position of ids
   *              information in the byte array above
   * @param count The number of target ids
   * @param message Message to send to remote worker
   * @return The size of messages for the worker.
   */
  private int addOneToAllMessage(
    WorkerInfo workerInfo, byte[] ids, int idPos, int count, M message) {
    // Get the data collection
    ByteArrayOneToAllMessages<I, M> workerData =
      oneToAllMsgCache[workerInfo.getTaskId()];
    if (workerData == null) {
      workerData = new ByteArrayOneToAllMessages<I, M>(
        getConf().getOutgoingMessageValueFactory());
      workerData.setConf(getConf());
      workerData.initialize(getSendWorkerInitialBufferSize(
        workerInfo.getTaskId()));
      oneToAllMsgCache[workerInfo.getTaskId()] = workerData;
    }
    workerData.add(ids, idPos, count, message);
    // Update the size of cached, outgoing data per worker
    oneToAllMsgSizes[workerInfo.getTaskId()] =
      workerData.getSize();
    return oneToAllMsgSizes[workerInfo.getTaskId()];
  }

  /**
   * Gets the one-to-all
   * messages for a worker and removes it from the cache.
   * Here the ByteArrayOneToAllMessages returned could be null.
   * But when invoking this method, we also check if the data size sent
   * to this worker is above the threshold. Therefore, it doesn't matter
   * if the result is null or not.
   *
   * @param workerInfo The target worker where one-to-all messages
   *                   go to.
   * @return ByteArrayOneToAllMessages that belong to the workerInfo
   */
  private ByteArrayOneToAllMessages<I, M>
  removeWorkerOneToAllMessages(WorkerInfo workerInfo) {
    ByteArrayOneToAllMessages<I, M> workerData =
      oneToAllMsgCache[workerInfo.getTaskId()];
    if (workerData != null) {
      oneToAllMsgCache[workerInfo.getTaskId()] = null;
      oneToAllMsgSizes[workerInfo.getTaskId()] = 0;
    }
    return workerData;
  }

  /**
   * Gets all the one-to-all
   * messages and removes them from the cache.
   *
   * @return All vertex messages for all workers
   */
  private PairList<WorkerInfo, ByteArrayOneToAllMessages<I, M>>
  removeAllOneToAllMessages() {
    PairList<WorkerInfo, ByteArrayOneToAllMessages<I, M>> allData =
      new PairList<WorkerInfo, ByteArrayOneToAllMessages<I, M>>();
    allData.initialize(oneToAllMsgCache.length);
    for (WorkerInfo workerInfo : getWorkerPartitions().keySet()) {
      ByteArrayOneToAllMessages<I, M> workerData =
        removeWorkerOneToAllMessages(workerInfo);
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
          PairList<Integer, ByteArrayVertexIdMessages<I, M>>
            workerMessages = removeWorkerMessages(workerInfoList[i]);
          writableRequest =
            new SendWorkerMessagesRequest<I, M>(workerMessages);
          totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
          clientProcessor.doRequest(workerInfoList[i], writableRequest);
          // Notify sending
          getServiceWorker().getGraphTaskManager().notifySentMessages();
        }
      } else if (idCounter[i] > 1) {
        serializedId = idSerializer[i].getByteArray();
        idSerializerPos = idSerializer[i].getPos();
        workerMessageSize = addOneToAllMessage(
          workerInfoList[i], serializedId, idSerializerPos, idCounter[i],
          message);

        if (LOG.isTraceEnabled()) {
          LOG.trace("sendMessageToAllRequest: Send bytes (" +
            message.toString() + ") to all targets in worker" +
            workerInfoList[i]);
        }
        totalMsgsSentInSuperstep += idCounter[i];
        if (workerMessageSize >= maxMessagesSizePerWorker) {
          ByteArrayOneToAllMessages<I, M> workerOneToAllMessages =
            removeWorkerOneToAllMessages(workerInfoList[i]);
          writableRequest =
            new SendWorkerOneToAllMessagesRequest<I, M>(
              workerOneToAllMessages, getConf());
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
    PairList<WorkerInfo, ByteArrayOneToAllMessages<I, M>>
    remainingOneToAllMessageCache =
      removeAllOneToAllMessages();
    PairList<WorkerInfo,
    ByteArrayOneToAllMessages<I, M>>.Iterator
    oneToAllMsgIterator = remainingOneToAllMessageCache.getIterator();
    while (oneToAllMsgIterator.hasNext()) {
      oneToAllMsgIterator.next();
      WritableRequest writableRequest =
        new SendWorkerOneToAllMessagesRequest<I, M>(
          oneToAllMsgIterator.getCurrentSecond(), getConf());
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(
        oneToAllMsgIterator.getCurrentFirst(), writableRequest);
    }
  }
}
