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
import org.apache.giraph.utils.VertexIdData;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract structure for caching data indexed by vertex id,
 * to be sent to workers in bulk. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <T> Data
 * @param <B> Specialization of {@link VertexIdData} for T
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public abstract class SendVertexIdDataCache<I extends WritableComparable, T,
    B extends VertexIdData<I, T>> extends SendDataCache<B> {
  /**
   * Constructor.
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param maxRequestSize Maximum request size (in bytes)
   * @param additionalRequestSize Additional request size (expressed as a
   *                              ratio of the average request size)
   */
  public SendVertexIdDataCache(ImmutableClassesGiraphConfiguration conf,
                               CentralizedServiceWorker<?, ?, ?> serviceWorker,
                               int maxRequestSize,
                               float additionalRequestSize) {
    super(conf, serviceWorker, maxRequestSize, additionalRequestSize);
  }

  /**
   * Create a new {@link VertexIdData} specialized for the use case.
   *
   * @return A new instance of {@link VertexIdData}
   */
  public abstract B createVertexIdData();

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
    VertexIdData<I, T> partitionData =
        getPartitionData(workerInfo, partitionId);
    int originalSize = partitionData.getSize();
    partitionData.add(destVertexId, data);
    // Update the size of cached, outgoing data per worker
    return incrDataSize(workerInfo.getTaskId(),
        partitionData.getSize() - originalSize);
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
    VertexIdData<I, T> partitionData =
        getPartitionData(workerInfo, partitionId);
    int originalSize = partitionData.getSize();
    partitionData.add(serializedId, idPos, data);
    // Update the size of cached, outgoing data per worker
    return incrDataSize(workerInfo.getTaskId(),
        partitionData.getSize() - originalSize);
  }

  /**
   * This method tries to get a partition data from the data cache.
   * If null, it will create one.
   *
   * @param workerInfo The remote worker destination
   * @param partitionId The remote Partition this message belongs to
   * @return The partition data in data cache
   */
  private VertexIdData<I, T> getPartitionData(WorkerInfo workerInfo,
                                                       int partitionId) {
    // Get the data collection
    B partitionData = getData(partitionId);
    if (partitionData == null) {
      partitionData = createVertexIdData();
      partitionData.setConf(getConf());
      partitionData.initialize(getInitialBufferSize(workerInfo.getTaskId()));
      setData(partitionId, partitionData);
    }

    return partitionData;
  }
}
