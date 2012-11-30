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

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.ByteArrayMessagesPerVertexStore;
import org.apache.giraph.comm.messages.OneMessagePerVertexStore;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.messages.BasicMessageStore;
import org.apache.giraph.comm.messages.DiskBackedMessageStore;
import org.apache.giraph.comm.messages.DiskBackedMessageStoreByPartition;
import org.apache.giraph.comm.messages.FlushableMessageStore;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.SequentialFileMessageStore;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Netty worker server that implement {@link WorkerServer} and contains
 * the actual {@link ServerData}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerServer<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerServer<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(NettyWorkerServer.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /** Netty server that does that actual I/O */
  private final NettyServer nettyServer;
  /** Server data storage */
  private final ServerData<I, V, E, M> serverData;

  /**
   * Constructor to start the server.
   *
   * @param conf Configuration
   * @param service Service to get partition mappings
   * @param context Mapper context
   */
  public NettyWorkerServer(ImmutableClassesGiraphConfiguration<I, V, E, M> conf,
      CentralizedServiceWorker<I, V, E, M> service,
      Mapper<?, ?, ?, ?>.Context context) {
    this.conf = conf;
    this.service = service;

    serverData =
        new ServerData<I, V, E, M>(conf, createMessageStoreFactory(), context);

    nettyServer = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory<I, V, E, M>(serverData),
        service.getWorkerInfo(), context);
    nettyServer.start();
  }

  /**
   * Decide which message store should be used for current application,
   * and create the factory for that store
   *
   * @return Message store factory
   */
  private MessageStoreFactory<I, M, MessageStoreByPartition<I, M>>
  createMessageStoreFactory() {
    boolean useOutOfCoreMessaging = conf.getBoolean(
        GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES,
        GiraphConfiguration.USE_OUT_OF_CORE_MESSAGES_DEFAULT);
    if (!useOutOfCoreMessaging) {
      if (conf.useCombiner()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("createMessageStoreFactory: " +
              "Using OneMessagePerVertexStore since combiner enabled");
        }
        return OneMessagePerVertexStore.newFactory(service, conf);
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("createMessageStoreFactory: " +
              "Using ByteArrayMessagesPerVertexStore " +
              "since there is no combiner");
        }
        return ByteArrayMessagesPerVertexStore.newFactory(service, conf);
      }
    } else {
      int maxMessagesInMemory = conf.getInt(
          GiraphConfiguration.MAX_MESSAGES_IN_MEMORY,
          GiraphConfiguration.MAX_MESSAGES_IN_MEMORY_DEFAULT);
      if (LOG.isInfoEnabled()) {
        LOG.info("createMessageStoreFactory: Using DiskBackedMessageStore, " +
            "maxMessagesInMemory = " + maxMessagesInMemory);
      }
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory =
          SequentialFileMessageStore.newFactory(conf);
      MessageStoreFactory<I, M, FlushableMessageStore<I, M>>
          partitionStoreFactory =
          DiskBackedMessageStore.newFactory(conf, fileStoreFactory);
      return DiskBackedMessageStoreByPartition.newFactory(service,
          maxMessagesInMemory, partitionStoreFactory);
    }
  }

  @Override
  public InetSocketAddress getMyAddress() {
    return nettyServer.getMyAddress();
  }

  @Override
  public void prepareSuperstep(GraphState<I, V, E, M> graphState) {
    serverData.prepareSuperstep();
    resolveMutations(graphState);
  }

  @Override
  public void resolveMutations(GraphState<I, V, E, M> graphState) {
    Set<I> resolveVertexIndexSet = Sets.newHashSet();
    // Keep track of the vertices which are not here but have received messages
    for (Integer partitionId : service.getPartitionStore().getPartitionIds()) {
      for (I vertexId : serverData.getCurrentMessageStore().
          getPartitionDestinationVertices(partitionId)) {
        Vertex<I, V, E, M> vertex = service.getVertex(vertexId);
        if (vertex == null) {
          if (!resolveVertexIndexSet.add(vertexId)) {
            throw new IllegalStateException(
                "prepareSuperstep: Already has missing vertex on this " +
                    "worker for " + vertexId);
          }
        }
      }
    }

    // Add any mutated vertex indices to be resolved
    for (I vertexIndex : serverData.getVertexMutations().keySet()) {
      if (!resolveVertexIndexSet.add(vertexIndex)) {
        throw new IllegalStateException(
            "prepareSuperstep: Already has missing vertex on this " +
                "worker for " + vertexIndex);
      }
    }

    // Resolve all graph mutations
    for (I vertexIndex : resolveVertexIndexSet) {
      VertexResolver<I, V, E, M> vertexResolver =
          conf.createVertexResolver(graphState);
      Vertex<I, V, E, M> originalVertex =
          service.getVertex(vertexIndex);

      VertexMutations<I, V, E, M> mutations = null;
      VertexMutations<I, V, E, M> vertexMutations =
          serverData.getVertexMutations().get(vertexIndex);
      if (vertexMutations != null) {
        synchronized (vertexMutations) {
          mutations = vertexMutations.copy();
        }
        serverData.getVertexMutations().remove(vertexIndex);
      }
      Vertex<I, V, E, M> vertex = vertexResolver.resolve(
          vertexIndex, originalVertex, mutations,
          serverData.getCurrentMessageStore().
              hasMessagesForVertex(vertexIndex));
      graphState.getContext().progress();

      if (LOG.isDebugEnabled()) {
        LOG.debug("prepareSuperstep: Resolved vertex index " +
            vertexIndex + " with original vertex " +
            originalVertex + ", returned vertex " + vertex +
            " on superstep " + service.getSuperstep() +
            " with mutations " +
            mutations);
      }

      Partition<I, V, E, M> partition =
          service.getPartition(vertexIndex);
      if (partition == null) {
        throw new IllegalStateException(
            "prepareSuperstep: No partition for index " + vertexIndex +
                " in " + service.getPartitionStore() + " should have been " +
                service.getVertexPartitionOwner(vertexIndex));
      }
      if (vertex != null) {
        partition.putVertex(vertex);
      } else if (originalVertex != null) {
        partition.removeVertex(originalVertex.getId());
      }
    }

    if (!serverData.getVertexMutations().isEmpty()) {
      throw new IllegalStateException("prepareSuperstep: Illegally " +
          "still has " + serverData.getVertexMutations().size() +
          " mutations left.");
    }
  }

  @Override
  public ServerData<I, V, E, M> getServerData() {
    return serverData;
  }

  @Override
  public void close() {
    nettyServer.stop();
  }
}
