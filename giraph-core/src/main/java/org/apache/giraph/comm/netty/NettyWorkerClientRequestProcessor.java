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

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.SendEdgeCache;
import org.apache.giraph.comm.SendMessageCache;
import org.apache.giraph.comm.SendMutationsCache;
import org.apache.giraph.comm.SendPartitionCache;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.requests.SendPartitionCurrentMessagesRequest;
import org.apache.giraph.comm.requests.SendPartitionMutationsRequest;
import org.apache.giraph.comm.requests.SendVertexRequest;
import org.apache.giraph.comm.requests.SendWorkerEdgesRequest;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.WorkerRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.util.PercentGauge;

import java.io.IOException;
import java.util.Map;

import static org.apache.giraph.conf.GiraphConstants.MAX_EDGE_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_MSG_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_MUTATIONS_PER_REQUEST;

/**
 * Aggregate requests and sends them to the thread-safe NettyClient.  This
 * class is not thread-safe and expected to be used and then thrown away after
 * a phase of communication has completed.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class NettyWorkerClientRequestProcessor<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    WorkerClientRequestProcessor<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(NettyWorkerClientRequestProcessor.class);
  /** Cached partitions of vertices to send */
  private final SendPartitionCache<I, V, E, M> sendPartitionCache;
  /** Cached map of partitions to vertex indices to messages */
  private final SendMessageCache<I, M> sendMessageCache;
  /** Cache of edges to be sent. */
  private final SendEdgeCache<I, E> sendEdgeCache;
  /** Cached map of partitions to vertex indices to mutations */
  private final SendMutationsCache<I, V, E, M> sendMutationsCache =
      new SendMutationsCache<I, V, E, M>();
  /** NettyClient that could be shared among one or more instances */
  private final WorkerClient<I, V, E, M> workerClient;
  /** Messages sent during the last superstep */
  private long totalMsgsSentInSuperstep = 0;
  /** Maximum size of messages per remote worker to cache before sending */
  private final int maxMessagesSizePerWorker;
  /** Maximum size of edges per remote worker to cache before sending. */
  private final int maxEdgesSizePerWorker;
  /** Maximum number of mutations per partition before sending */
  private final int maxMutationsPerPartition;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Server data from the server (used for local requests) */
  private final ServerData<I, V, E, M> serverData;

  // Per-Superstep Metrics
  /** Number of requests that went on the wire */
  private final Counter localRequests;
  /** Number of requests that were handled locally */
  private final Counter remoteRequests;

  /**
   * Constructor.
   *
   * @param context Context
   * @param conf Configuration
   * @param serviceWorker Service worker
   */
  public NettyWorkerClientRequestProcessor(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E, M> conf,
      CentralizedServiceWorker<I, V, E, M> serviceWorker) {
    this.workerClient = serviceWorker.getWorkerClient();
    this.configuration = conf;

    sendPartitionCache = new SendPartitionCache<I, V, E, M>(context, conf);
    sendMessageCache = new SendMessageCache<I, M>(conf, serviceWorker);
    sendEdgeCache = new SendEdgeCache<I, E>(conf, serviceWorker);
    maxMessagesSizePerWorker = MAX_MSG_REQUEST_SIZE.get(conf);
    maxEdgesSizePerWorker = MAX_EDGE_REQUEST_SIZE.get(conf);
    maxMutationsPerPartition = MAX_MUTATIONS_PER_REQUEST.get(conf);
    this.serviceWorker = serviceWorker;
    this.serverData = serviceWorker.getServerData();

    // Per-Superstep Metrics.
    // Since this object is not long lived we just initialize the metrics here.
    SuperstepMetricsRegistry smr = GiraphMetrics.get().perSuperstep();
    localRequests = smr.getCounter(MetricNames.LOCAL_REQUESTS);
    remoteRequests = smr.getCounter(MetricNames.REMOTE_REQUESTS);
    final Gauge<Long> totalRequests = smr.getGauge(MetricNames.TOTAL_REQUESTS,
        new Gauge<Long>() {
          @Override
          public Long value() {
            return localRequests.count() + remoteRequests.count();
          }
        }
    );
    smr.getGauge(MetricNames.PERCENT_LOCAL_REQUESTS, new PercentGauge() {
      @Override protected double getNumerator() {
        return localRequests.count();
      }

      @Override protected double getDenominator() {
        return totalRequests.value();
      }
    });
  }

  @Override
  public boolean sendMessageRequest(I destVertexId, M message) {
    PartitionOwner owner =
        serviceWorker.getVertexPartitionOwner(destVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendMessageRequest: Send bytes (" + message.toString() +
          ") to " + destVertexId + " on worker " + workerInfo);
    }
    ++totalMsgsSentInSuperstep;

    // Add the message to the cache
    int workerMessageSize = sendMessageCache.addMessage(
        workerInfo, partitionId, destVertexId, message);

    // Send a request if the cache of outgoing message to
    // the remote worker 'workerInfo' is full enough to be flushed
    if (workerMessageSize >= maxMessagesSizePerWorker) {
      PairList<Integer, ByteArrayVertexIdMessages<I, M>>
          workerMessages =
          sendMessageCache.removeWorkerMessages(workerInfo);
      WritableRequest writableRequest =
          new SendWorkerMessagesRequest<I, M>(workerMessages);
      doRequest(workerInfo, writableRequest);
      return true;
    }

    return false;
  }

  @Override
  public void sendPartitionRequest(WorkerInfo workerInfo,
                                   Partition<I, V, E, M> partition) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendVertexRequest: Sending to " + workerInfo +
          ", with partition " + partition);
    }

    WritableRequest vertexRequest =
        new SendVertexRequest<I, V, E, M>(partition);
    doRequest(workerInfo, vertexRequest);

    // Messages are stored separately
    if (serviceWorker.getSuperstep() != BspService.INPUT_SUPERSTEP) {
      sendPartitionMessages(workerInfo, partition);
    }
  }

  /**
   * Send all messages for a partition to another worker.
   *
   * @param workerInfo Worker to send the partition messages to
   * @param partition Partition whose messages to send
   */
  private void sendPartitionMessages(WorkerInfo workerInfo,
                                     Partition<I, V, E, M> partition) {
    final int partitionId = partition.getId();
    MessageStoreByPartition<I, M> messageStore =
        serverData.getCurrentMessageStore();
    ByteArrayVertexIdMessages<I, M> vertexIdMessages =
        new ByteArrayVertexIdMessages<I, M>();
    vertexIdMessages.setConf(configuration);
    vertexIdMessages.initialize();
    for (I vertexId :
        messageStore.getPartitionDestinationVertices(partitionId)) {
      try {
        // Messages cannot be re-used from this iterable, but add()
        // serializes the message, making this safe
        Iterable<M> messages = messageStore.getVertexMessages(vertexId);
        for (M message : messages) {
          vertexIdMessages.add(vertexId, message);
        }
      } catch (IOException e) {
        throw new IllegalStateException(
            "sendVertexRequest: Got IOException ", e);
      }
      if (vertexIdMessages.getSize() > maxMessagesSizePerWorker) {
        WritableRequest messagesRequest = new
            SendPartitionCurrentMessagesRequest<I, V, E, M>(
            partitionId, vertexIdMessages);
        doRequest(workerInfo, messagesRequest);
        vertexIdMessages =
            new ByteArrayVertexIdMessages<I, M>();
        vertexIdMessages.setConf(configuration);
        vertexIdMessages.initialize();
      }
    }
    if (!vertexIdMessages.isEmpty()) {
      WritableRequest messagesRequest = new
          SendPartitionCurrentMessagesRequest<I, V, E, M>(
          partitionId, vertexIdMessages);
      doRequest(workerInfo, messagesRequest);
    }
  }

  @Override
  public void sendVertexRequest(PartitionOwner partitionOwner,
                                Vertex<I, V, E, M> vertex) {
    Partition<I, V, E, M> partition =
        sendPartitionCache.addVertex(partitionOwner, vertex);
    if (partition == null) {
      return;
    }

    sendPartitionRequest(partitionOwner.getWorkerInfo(), partition);
  }

  @Override
  public void addEdgeRequest(I vertexIndex, Edge<I, E> edge) throws
      IOException {
    PartitionOwner partitionOwner =
        serviceWorker.getVertexPartitionOwner(vertexIndex);
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
  public boolean sendEdgeRequest(I sourceVertexId, Edge<I, E> edge)
    throws IOException {
    PartitionOwner owner =
        serviceWorker.getVertexPartitionOwner(sourceVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("sendEdgeRequest: Send bytes (" + edge.toString() +
          ") to " + sourceVertexId + " on worker " + workerInfo);
    }

    // Add the message to the cache
    int workerEdgesSize = sendEdgeCache.addEdge(
        workerInfo, partitionId, sourceVertexId, edge);

    // Send a request if the cache of outgoing edges to the remote worker is
    // full
    if (workerEdgesSize >= maxEdgesSizePerWorker) {
      PairList<Integer, ByteArrayVertexIdEdges<I, E>> workerEdges =
          sendEdgeCache.removeWorkerEdges(workerInfo);
      WritableRequest writableRequest =
          new SendWorkerEdgesRequest<I, E>(workerEdges);
      doRequest(workerInfo, writableRequest);
      return true;
    }

    return false;
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
      Map<I, VertexMutations<I, V, E, M>> partitionMutations =
          sendMutationsCache.removePartitionMutations(partitionId);
      WritableRequest writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              partitionId, partitionMutations);
      doRequest(partitionOwner.getWorkerInfo(), writableRequest);
    }
  }

  @Override
  public void removeEdgesRequest(I vertexIndex,
                                 I destinationVertexIndex) throws IOException {
    PartitionOwner partitionOwner =
        serviceWorker.getVertexPartitionOwner(vertexIndex);
    int partitionId = partitionOwner.getPartitionId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("removeEdgesRequest: Removing edge " +
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
        serviceWorker.getVertexPartitionOwner(vertex.getId());
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
        serviceWorker.getVertexPartitionOwner(vertexIndex);
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
    // Execute the remaining send partitions (if any)
    for (Map.Entry<PartitionOwner, Partition<I, V, E, M>> entry :
        sendPartitionCache.getOwnerPartitionMap().entrySet()) {
      sendPartitionRequest(entry.getKey().getWorkerInfo(), entry.getValue());
    }
    sendPartitionCache.clear();

    // Execute the remaining sends messages (if any)
    PairList<WorkerInfo, PairList<Integer,
        ByteArrayVertexIdMessages<I, M>>>
        remainingMessageCache = sendMessageCache.removeAllMessages();
    PairList<WorkerInfo,
        PairList<Integer, ByteArrayVertexIdMessages<I, M>>>.Iterator
        iterator = remainingMessageCache.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      WritableRequest writableRequest =
          new SendWorkerMessagesRequest<I, M>(
              iterator.getCurrentSecond());
      doRequest(iterator.getCurrentFirst(), writableRequest);
    }

    // Execute the remaining sends edges (if any)
    PairList<WorkerInfo, PairList<Integer,
        ByteArrayVertexIdEdges<I, E>>>
        remainingEdgeCache = sendEdgeCache.removeAllEdges();
    PairList<WorkerInfo,
        PairList<Integer, ByteArrayVertexIdEdges<I, E>>>.Iterator
        edgeIterator = remainingEdgeCache.getIterator();
    while (edgeIterator.hasNext()) {
      edgeIterator.next();
      WritableRequest writableRequest =
          new SendWorkerEdgesRequest<I, E>(
              edgeIterator.getCurrentSecond());
      doRequest(edgeIterator.getCurrentFirst(), writableRequest);
    }

    // Execute the remaining sends mutations (if any)
    Map<Integer, Map<I, VertexMutations<I, V, E, M>>> remainingMutationsCache =
        sendMutationsCache.removeAllPartitionMutations();
    for (Map.Entry<Integer, Map<I, VertexMutations<I, V, E, M>>> entry :
        remainingMutationsCache.entrySet()) {
      WritableRequest writableRequest =
          new SendPartitionMutationsRequest<I, V, E, M>(
              entry.getKey(), entry.getValue());
      PartitionOwner partitionOwner =
          serviceWorker.getVertexPartitionOwner(
              entry.getValue().keySet().iterator().next());
      doRequest(partitionOwner.getWorkerInfo(), writableRequest);
    }
  }

  @Override
  public long resetMessageCount() {
    long messagesSentInSuperstep = totalMsgsSentInSuperstep;
    totalMsgsSentInSuperstep = 0;
    return messagesSentInSuperstep;
  }

  /**
   * When doing the request, short circuit if it is local
   *
   * @param workerInfo Worker info
   * @param writableRequest Request to either submit or run locally
   */
  private void doRequest(WorkerInfo workerInfo,
                         WritableRequest writableRequest) {
    // If this is local, execute locally
    if (serviceWorker.getWorkerInfo().getTaskId() ==
        workerInfo.getTaskId()) {
      ((WorkerRequest) writableRequest).doRequest(serverData);
      localRequests.inc();
    } else {
      workerClient.sendWritableRequest(
          workerInfo.getTaskId(), writableRequest);
      remoteRequests.inc();
    }
  }
}
