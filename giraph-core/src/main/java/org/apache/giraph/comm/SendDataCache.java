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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;

/**
 * An abstract structure for caching data by partitions
 * to be sent to workers in bulk. Not thread-safe.
 *
 * @param <D> Data type of partition cache
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public abstract class SendDataCache<D> {
  /**
   * Internal cache of partitions (index) to their partition caches of
   * type D.
   */
  private final D[] dataCache;
  /** How big to initially make output streams for each worker's partitions */
  private final int[] initialBufferSizes;
  /** Service worker */
  private final CentralizedServiceWorker serviceWorker;
  /** Size of data (in bytes) for each worker */
  private final int[] dataSizes;
  /** Total number of workers */
  private final int numWorkers;
  /** List of partition ids belonging to a worker */
  private final Map<WorkerInfo, List<Integer>> workerPartitions =
      Maps.newHashMap();
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor.
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param maxRequestSize Maximum request size (in bytes)
   * @param additionalRequestSize Additional request size (expressed as a
   *                              ratio of the average request size)
   */
  public SendDataCache(ImmutableClassesGiraphConfiguration conf,
                       CentralizedServiceWorker<?, ?, ?> serviceWorker,
                       int maxRequestSize,
                       float additionalRequestSize) {
    this.conf = conf;
    this.serviceWorker = serviceWorker;
    int maxPartition = 0;
    for (PartitionOwner partitionOwner : serviceWorker.getPartitionOwners()) {
      List<Integer> workerPartitionIds =
          workerPartitions.get(partitionOwner.getWorkerInfo());
      if (workerPartitionIds == null) {
        workerPartitionIds = Lists.newArrayList();
        workerPartitions.put(partitionOwner.getWorkerInfo(),
            workerPartitionIds);
      }
      workerPartitionIds.add(partitionOwner.getPartitionId());
      maxPartition = Math.max(partitionOwner.getPartitionId(), maxPartition);
    }
    dataCache = (D[]) new Object[maxPartition + 1];

    int maxWorker = 0;
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      maxWorker = Math.max(maxWorker, workerInfo.getTaskId());
    }
    dataSizes = new int[maxWorker + 1];

    int initialRequestSize =
        (int) (maxRequestSize * (1 + additionalRequestSize));
    initialBufferSizes = new int[maxWorker + 1];
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      initialBufferSizes[workerInfo.getTaskId()] =
          initialRequestSize / workerPartitions.get(workerInfo).size();
    }
    numWorkers = maxWorker + 1;
  }

  /**
   * Gets the data for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the data
   * @return List of pairs (partitionId, ByteArrayVertexIdData),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, D>
  removeWorkerData(WorkerInfo workerInfo) {
    PairList<Integer, D> workerData = new PairList<Integer, D>();
    List<Integer> partitions = workerPartitions.get(workerInfo);
    workerData.initialize(partitions.size());
    for (Integer partitionId : partitions) {
      if (dataCache[partitionId] != null) {
        workerData.add(partitionId, (D) dataCache[partitionId]);
        dataCache[partitionId] = null;
      }
    }
    dataSizes[workerInfo.getTaskId()] = 0;
    return workerData;
  }

  /**
   * Gets all the data and removes it from the cache.
   *
   * @return All data for all vertices for all partitions
   */
  public PairList<WorkerInfo, PairList<Integer, D>> removeAllData() {
    PairList<WorkerInfo, PairList<Integer, D>> allData =
        new PairList<WorkerInfo, PairList<Integer, D>>();
    allData.initialize(dataSizes.length);
    for (WorkerInfo workerInfo : workerPartitions.keySet()) {
      PairList<Integer, D> workerData = removeWorkerData(workerInfo);
      if (!workerData.isEmpty()) {
        allData.add(workerInfo, workerData);
      }
      dataSizes[workerInfo.getTaskId()] = 0;
    }
    return allData;
  }

  /**
   * Get the data cache for a partition id
   *
   * @param partitionId Partition id
   * @return Data cache for a partition
   */
  public D getData(int partitionId) {
    return dataCache[partitionId];
  }

  /**
   * Set the data cache for a partition id
   *
   * @param partitionId Partition id
   * @param data Data to be set for a partition id
   */
  public void setData(int partitionId, D data) {
    dataCache[partitionId] = data;
  }

  /**
   * Get initial buffer size of a partition.
   *
   * @param partitionId Partition id
   * @return Initial buffer size of a partition
   */
  public int getInitialBufferSize(int partitionId) {
    return initialBufferSizes[partitionId];
  }

  /**
   * Increment the data size
   *
   * @param partitionId Partition id
   * @param size Size to increment by
   * @return new data size
   */
  public int incrDataSize(int partitionId, int size) {
    dataSizes[partitionId] += size;
    return dataSizes[partitionId];
  }

  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  /**
   * Get the service worker.
   *
   * @return CentralizedServiceWorker
   */
  protected CentralizedServiceWorker getServiceWorker() {
    return serviceWorker;
  }

  /**
   * Get the initial buffer size for the messages sent to a worker.
   *
   * @param taskId The task ID of a worker.
   * @return The initial buffer size for a worker.
   */
  protected int getSendWorkerInitialBufferSize(int taskId) {
    return initialBufferSizes[taskId];
  }

  protected int getNumWorkers() {
    return this.numWorkers;
  }

  protected Map<WorkerInfo, List<Integer>> getWorkerPartitions() {
    return workerPartitions;
  }
}
