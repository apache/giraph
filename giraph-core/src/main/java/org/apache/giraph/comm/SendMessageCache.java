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
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static org.apache.giraph.conf.GiraphConstants.ADDITIONAL_MSG_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_MSG_REQUEST_SIZE;

/**
 * Aggregates the messages to be sent to workers so they can be sent
 * in bulk.  Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class SendMessageCache<I extends WritableComparable, M extends Writable>
    extends SendCache<I, M, ByteArrayVertexIdMessages<I, M>> {
  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   */
  public SendMessageCache(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?, ?> serviceWorker) {
    super(conf, serviceWorker, MAX_MSG_REQUEST_SIZE.get(conf),
        ADDITIONAL_MSG_REQUEST_SIZE.get(conf));
  }

  @Override
  public ByteArrayVertexIdMessages<I, M> createByteArrayVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>();
  }

  /**
   * Add a message to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param message Message to send to remote worker
   * @return Size of messages for the worker.
   */
  public int addMessage(WorkerInfo workerInfo,
                        int partitionId, I destVertexId, M message) {
    return addData(workerInfo, partitionId, destVertexId, message);
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return List of pairs (partitionId, ByteArrayVertexIdMessages),
   *         where all partition ids belong to workerInfo
   */
  public PairList<Integer, ByteArrayVertexIdMessages<I, M>>
  removeWorkerMessages(WorkerInfo workerInfo) {
    return removeWorkerData(workerInfo);
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  public PairList<WorkerInfo, PairList<
      Integer, ByteArrayVertexIdMessages<I, M>>> removeAllMessages() {
    return removeAllData();
  }
}
