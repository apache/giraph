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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.apache.giraph.conf.GiraphConstants.ADDITIONAL_VERTEX_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_VERTEX_REQUEST_SIZE;

/**
 * Caches partition vertices prior to sending.  Aggregating these requests
 * will make larger, more efficient requests.  Not thread-safe.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class SendPartitionCache<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    SendDataCache<ExtendedDataOutput> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendPartitionCache.class);

  /**
   * Constructor.
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   */
  public SendPartitionCache(ImmutableClassesGiraphConfiguration<I, V, E> conf,
                            CentralizedServiceWorker<?, ?, ?> serviceWorker) {
    super(conf, serviceWorker, MAX_VERTEX_REQUEST_SIZE.get(conf),
        ADDITIONAL_VERTEX_REQUEST_SIZE.get(conf));
  }

  /**
   * Add a vertex to the cache.
   *
   * @param partitionOwner Partition owner of the vertex
   * @param vertex Vertex to add
   * @return Size of partitions for this worker
   */
  public int addVertex(PartitionOwner partitionOwner,
      Vertex<I, V, E> vertex) {
    // Get the data collection
    ExtendedDataOutput partitionData =
        getData(partitionOwner.getPartitionId());
    int taskId = partitionOwner.getWorkerInfo().getTaskId();
    int originalSize = 0;
    if (partitionData == null) {
      partitionData = getConf().createExtendedDataOutput(
          getInitialBufferSize(taskId));
      setData(partitionOwner.getPartitionId(), partitionData);
    } else {
      originalSize = partitionData.getPos();
    }
    try {
      WritableUtils.<I, V, E>writeVertexToDataOutput(
          partitionData, vertex, getConf());
    } catch (IOException e) {
      throw new IllegalStateException("addVertex: Failed to serialize", e);
    }

    // Update the size of cached, outgoing data per worker
    return incrDataSize(taskId, partitionData.getPos() - originalSize);
  }
}

