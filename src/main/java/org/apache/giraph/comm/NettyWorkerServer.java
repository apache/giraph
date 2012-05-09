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

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  private final Configuration conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /** Netty server that does that actual I/O */
  private final NettyServer<I, V, E, M> nettyServer;
  /** Server data storage */
  private final ServerData<I, V, E, M> serverData =
      new ServerData<I, V, E, M>();

  /**
   * Constructor to start the server.
   *
   * @param conf Configuration
   * @param service Service to get partition mappings
   */
  public NettyWorkerServer(Configuration conf,
      CentralizedServiceWorker<I, V, E, M> service) {
    this.conf = conf;
    this.service = service;
    nettyServer = new NettyServer<I, V, E, M>(conf, serverData);
    nettyServer.start();
  }

  @Override
  public int getPort() {
    return nettyServer.getMyAddress().getPort();
  }

  @Override
  public void prepareSuperstep() {
    // Assign the in messages to the vertices and keep track of the messages
    // that have no vertex here to be resolved later.  Ensure messages are
    // being sent to the right worker.
    Set<I> resolveVertexIndexSet = Sets.newHashSet();
    for (Entry<I, Collection<M>> entry :
        serverData.getTransientMessages().entrySet()) {
      synchronized (entry.getValue()) {
        if (entry.getValue().isEmpty()) {
          continue;
        }
        BasicVertex<I, V, E, M> vertex = service.getVertex(entry.getKey());
        if (vertex == null) {
          if (service.getPartition(entry.getKey()) == null) {
            throw new IllegalStateException("prepareSuperstep: No partition " +
                "for vertex index " + entry.getKey());
          }
          if (!resolveVertexIndexSet.add(entry.getKey())) {
            throw new IllegalStateException(
                "prepareSuperstep: Already has missing vertex on this " +
                    "worker for " + entry.getKey());
          }
        } else {
          service.assignMessagesToVertex(vertex, entry.getValue());
          entry.getValue().clear();
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
          BspUtils.createVertexResolver(
              conf, service.getGraphMapper().getGraphState());
      BasicVertex<I, V, E, M> originalVertex =
          service.getVertex(vertexIndex);
      Iterable<M> messages = null;
      if (originalVertex != null) {
        messages = originalVertex.getMessages();
      } else {
        Collection<M> transientMessages =
            serverData.getTransientMessages().get(vertexIndex);
        if (transientMessages != null) {
          synchronized (transientMessages) {
            messages = Lists.newArrayList(transientMessages);
          }
          serverData.getTransientMessages().remove(vertexIndex);
        }
      }

      VertexMutations<I, V, E, M> mutations = null;
      VertexMutations<I, V, E, M> vertexMutations =
          serverData.getVertexMutations().get(vertexIndex);
      if (vertexMutations != null) {
        synchronized (vertexMutations) {
          mutations = vertexMutations.copy();
        }
        serverData.getVertexMutations().remove(vertexIndex);
      }
      BasicVertex<I, V, E, M> vertex = vertexResolver.resolve(
          vertexIndex, originalVertex, mutations, messages);
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
            " in " + service.getPartitionMap() + " should have been " +
            service.getVertexPartitionOwner(vertexIndex));
      }
      if (vertex != null) {
        ((MutableVertex<I, V, E, M>) vertex).setVertexId(vertexIndex);
        partition.putVertex(vertex);
      } else if (originalVertex != null) {
        partition.removeVertex(originalVertex.getVertexId());
      }
    }

    serverData.getTransientMessages().clear();

    if (!serverData.getVertexMutations().isEmpty()) {
      throw new IllegalStateException("prepareSuperstep: Illegally " +
          "still has " + serverData.getVertexMutations().size() +
          " mutations left.");
    }
  }

  @Override
  public Map<Integer, Collection<BasicVertex<I, V, E, M>>>
  getInPartitionVertexMap() {
    return serverData.getPartitionVertexMap();
  }

  @Override
  public void close() {
    nettyServer.stop();
  }
}
