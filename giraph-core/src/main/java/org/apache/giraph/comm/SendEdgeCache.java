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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static org.apache.giraph.conf.GiraphConstants.ADDITIONAL_EDGE_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_EDGE_REQUEST_SIZE;

/**
 * Aggregates the edges to be sent to workers so they can be sent
 * in bulk.  Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class SendEdgeCache<I extends WritableComparable, E extends Writable>
    extends SendVertexIdDataCache<I, Edge<I, E>, VertexIdEdges<I, E>> {
  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   */
  public SendEdgeCache(ImmutableClassesGiraphConfiguration conf,
                       CentralizedServiceWorker<?, ?, ?> serviceWorker) {
    super(conf, serviceWorker, MAX_EDGE_REQUEST_SIZE.get(conf),
        ADDITIONAL_EDGE_REQUEST_SIZE.get(conf));
  }

  @Override
  public VertexIdEdges<I, E> createVertexIdData() {
    return new ByteArrayVertexIdEdges<I, E>();
  }

  /**
   * Add an edge to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this edge belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param edge Edge to send to remote worker
   * @return Size of edges for the worker.
   */
  public int addEdge(WorkerInfo workerInfo,
                     int partitionId, I destVertexId, Edge<I, E> edge) {
    return addData(workerInfo, partitionId, destVertexId, edge);
  }

  /**
   * Gets the edges for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the edges
   * @return List of pairs (partitionId, ByteArrayVertexIdEdges),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, VertexIdEdges<I, E>>
  removeWorkerEdges(WorkerInfo workerInfo) {
    return removeWorkerData(workerInfo);
  }

  /**
   * Gets all the edges and removes them from the cache.
   *
   * @return All vertex edges for all partitions
   */
  public PairList<WorkerInfo, PairList<Integer, VertexIdEdges<I, E>>>
  removeAllEdges() {
    return removeAllData();
  }
}
