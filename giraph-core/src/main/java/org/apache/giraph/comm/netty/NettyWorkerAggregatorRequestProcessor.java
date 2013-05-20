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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.aggregators.WorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.comm.aggregators.SendAggregatedValueCache;
import org.apache.giraph.comm.requests.SendAggregatorsToMasterRequest;
import org.apache.giraph.comm.requests.SendAggregatorsToWorkerRequest;
import org.apache.giraph.comm.requests.SendWorkerAggregatorsRequest;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Netty implementation of {@link WorkerAggregatorRequestProcessor}
 */
public class NettyWorkerAggregatorRequestProcessor
    implements WorkerAggregatorRequestProcessor {
  /** Progressable used to report progress */
  private final Progressable progressable;
  /** NettyClient that could be shared among one or more instances */
  private final WorkerClient<?, ?, ?> workerClient;
  /** Service worker */
  private final CentralizedServiceWorker<?, ?, ?> serviceWorker;
  /** Cached map of partition ids to serialized aggregator data */
  private final SendAggregatedValueCache sendAggregatedValueCache =
      new SendAggregatedValueCache();
  /** How big a single aggregator request can be */
  private final int maxBytesPerAggregatorRequest;

  /**
   * Constructor.
   *
   * @param progressable  Progressable used to report progress
   * @param configuration Configuration
   * @param serviceWorker Service worker
   */
  public NettyWorkerAggregatorRequestProcessor(
      Progressable progressable,
      ImmutableClassesGiraphConfiguration<?, ?, ?> configuration,
      CentralizedServiceWorker<?, ?, ?> serviceWorker) {
    this.serviceWorker = serviceWorker;
    this.workerClient = serviceWorker.getWorkerClient();
    this.progressable = progressable;
    maxBytesPerAggregatorRequest = configuration.getInt(
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST,
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST_DEFAULT);

  }

  @Override
  public boolean sendAggregatedValue(String aggregatorName,
      Writable aggregatedValue) throws IOException {
    WorkerInfo owner =
        AggregatorUtils.getOwner(aggregatorName,
            serviceWorker.getWorkerInfoList());
    if (isThisWorker(owner)) {
      return false;
    } else {
      int currentSize = sendAggregatedValueCache.addAggregator(
          owner.getTaskId(), aggregatorName, aggregatedValue);
      if (currentSize >= maxBytesPerAggregatorRequest) {
        flushAggregatorsToWorker(owner);
      }
      return true;
    }
  }

  @Override
  public void flush() throws IOException {
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      if (!isThisWorker(workerInfo)) {
        sendAggregatedValueCache.addCountAggregator(workerInfo.getTaskId());
        flushAggregatorsToWorker(workerInfo);
        progressable.progress();
      }
    }
    sendAggregatedValueCache.reset();
  }

  /**
   * Send aggregators from cache to worker.
   *
   * @param worker Worker which we want to send aggregators to
   */
  private void flushAggregatorsToWorker(WorkerInfo worker) {
    byte[] aggregatorData =
        sendAggregatedValueCache.removeAggregators(worker.getTaskId());
    workerClient.sendWritableRequest(worker.getTaskId(),
        new SendWorkerAggregatorsRequest(aggregatorData,
            serviceWorker.getWorkerInfo().getTaskId()));
  }

  @Override
  public void sendAggregatedValuesToMaster(
      byte[] aggregatorData) throws IOException {
    workerClient.sendWritableRequest(serviceWorker.getMasterInfo().getTaskId(),
        new SendAggregatorsToMasterRequest(aggregatorData));
  }

  @Override
  public void distributeAggregators(
      Iterable<byte[]> aggregatorDataList) throws IOException {
    for (byte[] aggregatorData : aggregatorDataList) {
      SendAggregatorsToWorkerRequest request =
          new SendAggregatorsToWorkerRequest(aggregatorData,
              serviceWorker.getWorkerInfo().getTaskId());
      for (WorkerInfo worker : serviceWorker.getWorkerInfoList()) {
        if (!isThisWorker(worker)) {
          workerClient.sendWritableRequest(worker.getTaskId(), request);
        }
        progressable.progress();
      }
    }
  }

  /**
   * Check if workerInfo describes current worker.
   *
   * @param workerInfo Worker to check
   * @return True iff workerInfo corresponds to current worker.
   */
  private boolean isThisWorker(WorkerInfo workerInfo) {
    return serviceWorker.getWorkerInfo().getTaskId() == workerInfo.getTaskId();
  }
}
