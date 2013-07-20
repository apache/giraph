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
import org.apache.giraph.utils.ByteArrayVertexIdData;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;
import java.util.Map;

/**
 * An abstract structure for caching data indexed by vertex id,
 * to be sent to workers in bulk. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <T> Data
 * @param <B> Specialization of {@link ByteArrayVertexIdData} for T
 */
@SuppressWarnings("unchecked")
public abstract class SendCache<I extends WritableComparable, T,
    B extends ByteArrayVertexIdData<I, T>> {
  /** How big to initially make output streams for each worker's partitions */
  private final int[] initialBufferSizes;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** Service worker */
  private final CentralizedServiceWorker serviceWorker;
  /** Internal cache */
  private final ByteArrayVertexIdData<I, T>[] dataCache;
  /** Size of data (in bytes) for each worker */
  private final int[] dataSizes;
  /** Total number of workers */
  private final int numWorkers;
  /** List of partition ids belonging to a worker */
  private final Map<WorkerInfo, List<Integer>> workerPartitions =
      Maps.newHashMap();

  /**
   * Constructor.
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param maxRequestSize Maximum request size (in bytes)
   * @param additionalRequestSize Additional request size (expressed as a
   *                              ratio of the average request size)
   */
  public SendCache(ImmutableClassesGiraphConfiguration conf,
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
    dataCache = new ByteArrayVertexIdData[maxPartition + 1];

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
   * Create a new {@link ByteArrayVertexIdData} specialized for the use case.
   *
   * @return A new instance of {@link ByteArrayVertexIdData}
   */
  public abstract B createByteArrayVertexIdData();

  /**
   * Add data to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param data Data to send to remote worker
   * @return Size of messages for the worker.
   */
  public int addData(WorkerInfo workerInfo,
                     int partitionId, I destVertexId, T data) {
    // Get the data collection
    ByteArrayVertexIdData<I, T> partitionData =
      getPartitionData(workerInfo, partitionId);
    int originalSize = partitionData.getSize();
    partitionData.add(destVertexId, data);
    // Update the size of cached, outgoing data per worker
    dataSizes[workerInfo.getTaskId()] +=
      partitionData.getSize() - originalSize;
    return dataSizes[workerInfo.getTaskId()];
  }

  /**
   * This method is similar to the method above,
   * but use a serialized id to replace original I type
   * destVertexId.
   *
   * @param workerInfo The remote worker destination
   * @param partitionId The remote Partition this message belongs to
   * @param serializedId The byte array to store the serialized target vertex id
   * @param idPos The length of bytes of serialized id in the byte array above
   * @param data Data to send to remote worker
   * @return The number of bytes added to the target worker
   */
  public int addData(WorkerInfo workerInfo, int partitionId,
    byte[] serializedId, int idPos, T data) {
    // Get the data collection
    ByteArrayVertexIdData<I, T> partitionData =
      getPartitionData(workerInfo, partitionId);
    int originalSize = partitionData.getSize();
    partitionData.add(serializedId, idPos, data);
    // Update the size of cached, outgoing data per worker
    dataSizes[workerInfo.getTaskId()] +=
      partitionData.getSize() - originalSize;
    return dataSizes[workerInfo.getTaskId()];
  }

  /**
   * This method tries to get a partition data from the data cache.
   * If null, it will create one.
   *
   * @param workerInfo The remote worker destination
   * @param partitionId The remote Partition this message belongs to
   * @return The partition data in data cache
   */
  private ByteArrayVertexIdData<I, T> getPartitionData(WorkerInfo workerInfo,
    int partitionId) {
    ByteArrayVertexIdData<I, T> partitionData = dataCache[partitionId];
    if (partitionData == null) {
      partitionData = createByteArrayVertexIdData();
      partitionData.setConf(conf);
      partitionData.initialize(initialBufferSizes[workerInfo.getTaskId()]);
      dataCache[partitionId] = partitionData;
    }
    return partitionData;
  }

  /**
   * Gets the data for a worker and removes it from the cache.
   *
   * @param workerInfo The address of the worker who owns the data
   *                   partitions that are receiving the data
   * @return List of pairs (partitionId, ByteArrayVertexIdData),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, B>
  removeWorkerData(WorkerInfo workerInfo) {
    PairList<Integer, B> workerData = new PairList<Integer, B>();
    List<Integer> partitions = workerPartitions.get(workerInfo);
    workerData.initialize(partitions.size());
    for (Integer partitionId : partitions) {
      if (dataCache[partitionId] != null) {
        workerData.add(partitionId, (B) dataCache[partitionId]);
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
  public PairList<WorkerInfo, PairList<Integer, B>> removeAllData() {
    PairList<WorkerInfo, PairList<Integer, B>> allData =
        new PairList<WorkerInfo, PairList<Integer, B>>();
    allData.initialize(dataSizes.length);
    for (WorkerInfo workerInfo : workerPartitions.keySet()) {
      PairList<Integer, B> workerData = removeWorkerData(workerInfo);
      if (!workerData.isEmpty()) {
        allData.add(workerInfo, workerData);
      }
      dataSizes[workerInfo.getTaskId()] = 0;
    }
    return allData;
  }

  protected ImmutableClassesGiraphConfiguration getConf() {
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
