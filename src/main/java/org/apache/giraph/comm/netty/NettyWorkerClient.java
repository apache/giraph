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

package org.apache.giraph.comm.netty;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.Iterator;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.SendMessageCache;
import org.apache.giraph.comm.SendMutationsCache;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.requests.SendPartitionCurrentMessagesRequest;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.SendPartitionMutationsRequest;
import org.apache.giraph.comm.requests.SendVertexRequest;
import org.apache.giraph.comm.requests.WorkerRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
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
  /** signal for getInetSocketAddress() to use WorkerInfo's address */
  private static final int NO_PARTITION_ID = Integer.MIN_VALUE;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Netty client that does that actual I/O */
  private final NettyClient nettyClient;
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
  /** Maximum number of messages per remote worker to cache before sending */
  private final int maxMessagesPerWorker;
  /** Maximum number of mutations per partition before sending */
  private final int maxMutationsPerPartition;
  /** Maximum number of attempts to resolve an address*/
  private final int maxResolveAddressAttempts;
  /** Messages sent during the last superstep */
  private long totalMsgsSentInSuperstep = 0;
  /** Server data from the server */
  private final ServerData<I, V, E, M> serverData;

  /**
   * Only constructor.
   *
   * @param context Context from mapper
   * @param configuration Configuration
   * @param service Used to get partition mapping
   * @param serverData Server data (used for local requests)
   */
  public NettyWorkerClient(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      CentralizedServiceWorker<I, V, E, M> service,
      ServerData<I, V, E, M> serverData) {
    this.nettyClient = new NettyClient(context, configuration);
    this.conf = configuration;
    this.service = service;
    maxMessagesPerWorker = conf.getInt(
        GiraphConfiguration.MSG_SIZE,
        GiraphConfiguration.MSG_SIZE_DEFAULT);
    maxMutationsPerPartition = conf.getInt(
        GiraphConfiguration.MAX_MUTATIONS_PER_REQUEST,
        GiraphConfiguration.MAX_MUTATIONS_PER_REQUEST_DEFAULT);
    maxResolveAddressAttempts = conf.getInt(
        GiraphConfiguration.MAX_RESOLVE_ADDRESS_ATTEMPTS,
        GiraphConfiguration.MAX_RESOLVE_ADDRESS_ATTEMPTS_DEFAULT);
    sendMessageCache = new SendMessageCache<I, M>(conf);
    sendMutationsCache = new SendMutationsCache<I, V, E, M>();
    this.serverData = serverData;
  }

  @Override
  public void fixPartitionIdToSocketAddrMap() {
    // 1. Fix all the cached inet addresses (remove all changed entries)
    // 2. Connect to any new RPC servers
    Set<InetSocketAddress> addresses =
        Sets.newHashSetWithExpectedSize(service.getPartitionOwners().size());
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

      // No need to connect to myself
      if (service.getWorkerInfo().getTaskId() !=
          partitionOwner.getWorkerInfo().getTaskId()) {
        addresses.add(getInetSocketAddress(partitionOwner.getWorkerInfo(),
            partitionOwner.getPartitionId()));
      }
    }
    boolean useNetty = conf.getBoolean(GiraphConfiguration.USE_NETTY,
        GiraphConfiguration.USE_NETTY_DEFAULT);
    if (useNetty) {
      addresses.add(service.getMasterInfo().getInetSocketAddress());
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
    InetSocketAddress address = partitionIndexAddressMap.get(partitionId);
    if (address == null) {
      address = resolveAddress(workerInfo.getInetSocketAddress());
      if (partitionId != NO_PARTITION_ID) {
        // Only cache valid partition ids
        partitionIndexAddressMap.put(partitionId, address);
      }
    }

    return address;
  }

  /**
   * Utility method for getInetSocketAddress()
   * @param address the address we are attempting to resolve
   * @return the successfully resolved address.
   * @throws IllegalStateException if the address is not resolved
   *         in <code>maxResolveAddressAttempts</code> tries.
   */
  private InetSocketAddress resolveAddress(InetSocketAddress address) {
    int resolveAttempts = 0;
    while (address.isUnresolved() &&
      resolveAttempts < maxResolveAddressAttempts) {
      ++resolveAttempts;
      LOG.warn("resolveAddress: Failed to resolve " + address +
        " on attempt " + resolveAttempts + " of " +
        maxResolveAddressAttempts + " attempts, sleeping for 5 seconds");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn("resolveAddress: Interrupted.", e);
      }
    }
    if (resolveAttempts >= maxResolveAddressAttempts) {
      throw new IllegalStateException("resolveAddress: Couldn't " +
        "resolve " + address + " in " +  resolveAttempts + " tries.");
    }
    return address;
  }

  /**
   * When doing the request, short circuit if it is local
   *
   * @param workerInfo Worker info
   * @param remoteServerAddress Remote server address (checked against local)
   * @param writableRequest Request to either submit or run locally
   */
  private void doRequest(WorkerInfo workerInfo,
                         InetSocketAddress remoteServerAddress,
                         WritableRequest writableRequest) {
    // If this is local, execute locally
    if (service.getWorkerInfo().getTaskId() ==
        workerInfo.getTaskId()) {
      ((WorkerRequest) writableRequest).doRequest(serverData);
    } else {
      nettyClient.sendWritableRequest(
          workerInfo.getTaskId(), remoteServerAddress, writableRequest);
    }
  }

  @Override
  public void sendMessageRequest(I destVertexId, M message) {
    PartitionOwner owner = service.getVertexPartitionOwner(destVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendMessageRequest: Send bytes (" + message.toString() +
          ") to " + destVertexId + " on worker " + workerInfo);
    }
    ++totalMsgsSentInSuperstep;

    // Add the message to the cache
    int workerMessageCount = sendMessageCache
      .addMessage(workerInfo, partitionId, destVertexId, message);

    // Send a request if the cache of outgoing message to
    // the remote worker 'workerInfo' is full enough to be flushed
    if (workerMessageCount >= maxMessagesPerWorker) {
      Map<Integer, Map<I, Collection<M>>> workerMessages =
        sendMessageCache.removeWorkerMessages(workerInfo);
      InetSocketAddress remoteWorkerAddress =
        getInetSocketAddress(workerInfo, partitionId);
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, V, E, M>(workerMessages);
      doRequest(workerInfo, remoteWorkerAddress, writableRequest);
    }
  }

  @Override
  public void sendPartitionRequest(WorkerInfo workerInfo,
                                   Partition<I, V, E, M> partition) {
    final int partitionId = partition.getId();
    InetSocketAddress remoteServerAddress =
        getInetSocketAddress(workerInfo, partitionId);
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendPartitionRequest: Sending to " +
          remoteServerAddress +
          " from " + workerInfo + ", with partition " + partition);
    }

    WritableRequest vertexRequest =
        new SendVertexRequest<I, V, E, M>(partitionId,
            partition.getVertices());
    doRequest(workerInfo, remoteServerAddress, vertexRequest);

    // Messages are stored separately
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
      if (messagesInMap > maxMessagesPerWorker) {
        WritableRequest messagesRequest = new
            SendPartitionCurrentMessagesRequest<I, V, E, M>(partitionId, map);
        doRequest(workerInfo, remoteServerAddress, messagesRequest);
        map.clear();
        messagesInMap = 0;
      }
    }
    if (!map.isEmpty()) {
      WritableRequest messagesRequest = new
          SendPartitionCurrentMessagesRequest<I, V, E, M>(partitionId, map);
      doRequest(workerInfo, remoteServerAddress, messagesRequest);
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
      WritableRequest writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              partitionId, partitionMutations);
      doRequest(partitionOwner.getWorkerInfo(), remoteServerAddress,
          writableRequest);
    }
  }

  @Override
  public void addEdgeRequest(I vertexIndex, Edge<I, E> edge) throws
      IOException {
    PartitionOwner partitionOwner =
        service.getVertexPartitionOwner(vertexIndex);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("addEdgeRequest: Sending edge " + edge + " for index " +
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("removeEdgeRequest: Removing edge " +
          destinationVertexIndex +
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("addVertexRequest: Sending vertex " + vertex +
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("removeVertexRequest: Removing vertex index " +
          vertexIndex + " from partition " + partitionId);
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
    Map<WorkerInfo, Map<Integer, Map<I, Collection<M>>>> remainingMessageCache =
        sendMessageCache.removeAllMessages();
    for (Entry<WorkerInfo, Map<Integer, Map<I, Collection<M>>>> entry :
      remainingMessageCache.entrySet()) {
      Iterator<Integer> cachedPartitionId =
        entry.getValue().keySet().iterator();
      final int partitionId = cachedPartitionId.hasNext() ?
        cachedPartitionId.next() : NO_PARTITION_ID;
      InetSocketAddress remoteWorkerAddress =
        getInetSocketAddress(entry.getKey(), partitionId);
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, V, E, M>(entry.getValue());
      doRequest(entry.getKey(), remoteWorkerAddress, writableRequest);
    }

    // Execute the remaining sends mutations (if any)
    Map<Integer, Map<I, VertexMutations<I, V, E, M>>> remainingMutationsCache =
        sendMutationsCache.removeAllPartitionMutations();
    for (Entry<Integer, Map<I, VertexMutations<I, V, E, M>>> entry :
        remainingMutationsCache.entrySet()) {
      WritableRequest writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              entry.getKey(), entry.getValue());
      PartitionOwner partitionOwner =
          service.getVertexPartitionOwner(
              entry.getValue().keySet().iterator().next());
      InetSocketAddress remoteServerAddress =
          getInetSocketAddress(partitionOwner.getWorkerInfo(),
              partitionOwner.getPartitionId());
      doRequest(partitionOwner.getWorkerInfo(), remoteServerAddress,
          writableRequest);
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
