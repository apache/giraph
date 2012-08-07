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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.messages.SendPartitionCurrentMessagesRequest;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Takes users facing APIs in {@link WorkerClient} and implements them
 * using the available {@link WritableRequest} objects.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerClient<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    WorkerClient<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(NettyWorkerClient.class);
  /** Hadoop configuration */
  private final Configuration conf;
  /** Netty client that does that actual I/O */
  private final NettyClient<I, V, E, M> nettyClient;
  /** Centralized service, needed to get vertex ranges */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /**
   * Cached map of partition ids to remote socket address.
   */
  private final Map<Integer, InetSocketAddress> partitionIndexAddressMap =
      new ConcurrentHashMap<Integer, InetSocketAddress>();
  /**
   * Cached map of partitions to vertex indices to messages
   */
  private final SendMessageCache<I, M> sendMessageCache;
  /**
   * Cached map of partitions to vertex indices to mutations
   */
  private final SendMutationsCache<I, V, E, M> sendMutationsCache;
  /** Maximum number of messages per partition before sending */
  private final int maxMessagesPerPartition;
  /** Maximum number of mutations per partition before sending */
  private final int maxMutationsPerPartition;
  /** Messages sent during the last superstep */
  private long totalMsgsSentInSuperstep = 0;
  /** Server data from the server */
  private final ServerData<I, V, E, M> serverData;

  /**
   * Only constructor.
   *
   * @param context Context from mapper
   * @param service Used to get partition mapping
   * @param serverData Server data (used for local requests)
   */
  public NettyWorkerClient(Mapper<?, ?, ?, ?>.Context context,
                           CentralizedServiceWorker<I, V, E, M> service,
                           ServerData<I, V, E, M> serverData) {
    this.nettyClient = new NettyClient<I, V, E, M>(context);
    this.conf = context.getConfiguration();
    this.service = service;
    maxMessagesPerPartition = conf.getInt(GiraphJob.MSG_SIZE,
        GiraphJob.MSG_SIZE_DEFAULT);
    maxMutationsPerPartition = conf.getInt(GiraphJob.MAX_MUTATIONS_PER_REQUEST,
        GiraphJob.MAX_MUTATIONS_PER_REQUEST_DEFAULT);
    sendMessageCache = new SendMessageCache<I, M>(conf);
    sendMutationsCache = new SendMutationsCache<I, V, E, M>();
    this.serverData = serverData;
  }

  @Override
  public void fixPartitionIdToSocketAddrMap() {
    // 1. Fix all the cached inet addresses (remove all changed entries)
    // 2. Connect to any new RPC servers
    List<InetSocketAddress> addresses =
        Lists.newArrayListWithCapacity(service.getPartitionOwners().size());
    for (PartitionOwner partitionOwner : service.getPartitionOwners()) {
      InetSocketAddress address =
          partitionIndexAddressMap.get(
              partitionOwner.getPartitionId());
      if (address != null &&
          (!address.getHostName().equals(
              partitionOwner.getWorkerInfo().getHostname()) ||
              address.getPort() !=
              partitionOwner.getWorkerInfo().getPort())) {
        if (LOG.isInfoEnabled()) {
          LOG.info("fixPartitionIdToSocketAddrMap: " +
              "Partition owner " +
              partitionOwner + " changed from " +
              address);
        }
        partitionIndexAddressMap.remove(
            partitionOwner.getPartitionId());
      }
      addresses.add(partitionOwner.getWorkerInfo().getHostnamePort());
    }
    nettyClient.connectAllAddresses(addresses);
  }

  /**
   * Fill the socket address cache for the worker info and its partition.
   *
   * @param workerInfo Worker information to get the socket address
   * @param partitionId Partition id to look up.
   * @return address of the vertex range server containing this vertex
   */
  private InetSocketAddress getInetSocketAddress(WorkerInfo workerInfo,
      int partitionId) {
    InetSocketAddress address =
        partitionIndexAddressMap.get(partitionId);
    if (address == null) {
      address = workerInfo.getHostnamePort();
      partitionIndexAddressMap.put(partitionId, address);
    }

    return address;
  }

  /**
   * Fill the socket address cache for the partition owner.
   *
   * @param destVertex vertex to be sent
   * @return address of the vertex range server containing this vertex
   */
  private InetSocketAddress getInetSocketAddress(I destVertex) {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(destVertex);
    return getInetSocketAddress(partitionOwner.getWorkerInfo(),
        partitionOwner.getPartitionId());
  }

  /**
   * When doing the request, short circuit if it is local
   *
   * @param remoteServerAddress Remote server address (checked against local)
   * @param writableRequest Request to either submit or run locally
   */
  private void doRequest(InetSocketAddress remoteServerAddress,
                         WritableRequest<I, V, E, M> writableRequest) {
    // If this is local, execute locally
    if (service.getWorkerInfo().getHostnamePort().equals(
        remoteServerAddress)) {
      writableRequest.doRequest(serverData);
    } else {
      nettyClient.sendWritableRequest(remoteServerAddress, writableRequest);
    }
  }

  @Override
  public void sendMessageRequest(I destVertexId, M message) {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(destVertexId);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("sendMessageRequest: Send bytes (" + message.toString() +
          ") to " + destVertexId + " with partition " + partitionId);
    }
    ++totalMsgsSentInSuperstep;

    // Add the message to the cache
    int partitionMessageCount =
        sendMessageCache.addMessage(partitionId, destVertexId, message);

    // Send a request if enough messages are there for a partition
    if (partitionMessageCount >= maxMessagesPerPartition) {
      InetSocketAddress remoteServerAddress =
          getInetSocketAddress(partitionOwner.getWorkerInfo(), partitionId);
      Map<I, Collection<M>> partitionMessages =
          sendMessageCache.removePartitionMessages(partitionId);
      WritableRequest<I, V, E, M> writableRequest =
          new SendPartitionMessagesRequest<I, V, E, M>(
              partitionId, partitionMessages);
      doRequest(remoteServerAddress, writableRequest);
    }
  }

  @Override
  public void sendPartitionRequest(WorkerInfo workerInfo,
                                   Partition<I, V, E, M> partition) {
    InetSocketAddress remoteServerAddress =
        getInetSocketAddress(workerInfo, partition.getId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("sendPartitionRequest: Sending to " + remoteServerAddress +
          " from " + workerInfo + ", with partition " + partition);
    }

    int partitionId = partition.getId();
    WritableRequest<I, V, E, M> vertexRequest =
        new SendVertexRequest<I, V, E, M>(partitionId,
            partition.getVertices());
    doRequest(remoteServerAddress, vertexRequest);

    // messages are stored separately
    MessageStoreByPartition<I, M> messageStore =
        service.getServerData().getCurrentMessageStore();
    Map<I, Collection<M>> map = Maps.newHashMap();
    int messagesInMap = 0;
    for (I vertexId :
        messageStore.getPartitionDestinationVertices(partitionId)) {
      try {
        Collection<M> messages = messageStore.getVertexMessages(vertexId);
        map.put(vertexId, messages);
        messagesInMap += messages.size();
      } catch (IOException e) {
        throw new IllegalStateException(
            "sendPartitionReq: Got IOException ", e);
      }
      if (messagesInMap > maxMessagesPerPartition) {
        WritableRequest<I, V, E, M> messagesRequest = new
            SendPartitionCurrentMessagesRequest<I, V, E, M>(partitionId, map);
        doRequest(remoteServerAddress, messagesRequest);
        map.clear();
        messagesInMap = 0;
      }
    }
    if (!map.isEmpty()) {
      WritableRequest<I, V, E, M> messagesRequest = new
          SendPartitionCurrentMessagesRequest<I, V, E, M>(partitionId, map);
      doRequest(remoteServerAddress, messagesRequest);
    }
  }

    /**
    * Send a mutations request if the maximum number of mutations per partition
    * was met.
    *
    * @param partitionId Partition id
    * @param partitionOwner Owner of the partition
    * @param partitionMutationCount Number of mutations for this partition
    */
  private void sendMutationsRequestIfFull(
      int partitionId, PartitionOwner partitionOwner,
      int partitionMutationCount) {
    // Send a request if enough mutations are there for a partition
    if (partitionMutationCount >= maxMutationsPerPartition) {
      InetSocketAddress remoteServerAddress =
          getInetSocketAddress(partitionOwner.getWorkerInfo(), partitionId);
      Map<I, VertexMutations<I, V, E, M>> partitionMutations =
          sendMutationsCache.removePartitionMutations(partitionId);
      WritableRequest<I, V, E, M> writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              partitionId, partitionMutations);
      doRequest(remoteServerAddress, writableRequest);
    }
  }

  @Override
  public void addEdgeRequest(I vertexIndex, Edge<I, E> edge) throws
      IOException {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(vertexIndex);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("addEdgeRequest: Sending edge " + edge + " for index " +
          vertexIndex + " with partition " + partitionId);
    }

    // Add the message to the cache
    int partitionMutationCount =
        sendMutationsCache.addEdgeMutation(partitionId, vertexIndex, edge);

    sendMutationsRequestIfFull(
        partitionId, partitionOwner, partitionMutationCount);
  }

  @Override
  public void removeEdgeRequest(I vertexIndex,
                                I destinationVertexIndex) throws IOException {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(vertexIndex);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("removeEdgeRequest: Removing edge " + destinationVertexIndex +
          " for index " + vertexIndex + " with partition " + partitionId);
    }

    // Add the message to the cache
    int partitionMutationCount =
        sendMutationsCache.removeEdgeMutation(
            partitionId, vertexIndex, destinationVertexIndex);

    sendMutationsRequestIfFull(
        partitionId, partitionOwner, partitionMutationCount);
  }

  @Override
  public void addVertexRequest(Vertex<I, V, E, M> vertex) throws IOException {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(vertex.getId());
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("addVertexRequest: Sending vertex " + vertex +
          " to partition " + partitionId);
    }

    // Add the message to the cache
    int partitionMutationCount =
        sendMutationsCache.addVertexMutation(partitionId, vertex);

    sendMutationsRequestIfFull(
        partitionId, partitionOwner, partitionMutationCount);
  }

  @Override
  public void removeVertexRequest(I vertexIndex) throws IOException {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(vertexIndex);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("removeVertexRequest: Removing vertex index " + vertexIndex +
          " from partition " + partitionId);
    }

    // Add the message to the cache
    int partitionMutationCount =
        sendMutationsCache.removeVertexMutation(partitionId, vertexIndex);

    sendMutationsRequestIfFull(
        partitionId, partitionOwner, partitionMutationCount);
  }

  @Override
  public void flush() throws IOException {
    // Execute the remaining sends messages (if any)
    Map<Integer, Map<I, Collection<M>>> remainingMessageCache =
        sendMessageCache.removeAllPartitionMessages();
    for (Entry<Integer, Map<I, Collection<M>>> entry :
        remainingMessageCache.entrySet()) {
      WritableRequest<I, V, E, M> writableRequest =
          new SendPartitionMessagesRequest<I, V, E, M>(
              entry.getKey(), entry.getValue());
      InetSocketAddress remoteServerAddress =
          getInetSocketAddress(entry.getValue().keySet().iterator().next());
      doRequest(remoteServerAddress, writableRequest);
    }

    // Execute the remaining sends mutations (if any)
    Map<Integer, Map<I, VertexMutations<I, V, E, M>>> remainingMutationsCache =
        sendMutationsCache.removeAllPartitionMutations();
    for (Entry<Integer, Map<I, VertexMutations<I, V, E, M>>> entry :
        remainingMutationsCache.entrySet()) {
      WritableRequest<I, V, E, M> writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              entry.getKey(), entry.getValue());
      InetSocketAddress remoteServerAddress =
          getInetSocketAddress(entry.getValue().keySet().iterator().next());
      doRequest(remoteServerAddress, writableRequest);
    }

    nettyClient.waitAllRequests();
  }

  @Override
  public long resetMessageCount() {
    long messagesSentInSuperstep = totalMsgsSentInSuperstep;
    totalMsgsSentInSuperstep = 0;
    return messagesSentInSuperstep;
  }

  @Override
  public void closeConnections() throws IOException {
    nettyClient.stop();
  }

  @Override
  public void setup() {
    fixPartitionIdToSocketAddrMap();
  }
}
