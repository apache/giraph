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
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.comm.aggregators.SendAggregatorCache;
import org.apache.giraph.comm.requests.SendAggregatorsToOwnerRequest;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Netty implementation of {@link MasterClient}
 */
public class NettyMasterClient implements MasterClient {
  /** Netty client that does the actual I/O */
  private final NettyClient nettyClient;
  /** Worker information for current superstep */
  private CentralizedServiceMaster<?, ?, ?> service;
  /** Cached map of partition ids to serialized aggregator data */
  private final SendAggregatorCache sendAggregatorCache =
      new SendAggregatorCache();
  /** How big a single aggregator request can be */
  private final int maxBytesPerAggregatorRequest;
  /** Progressable used to report progress */
  private final Progressable progressable;

  /**
   * Constructor
   *
   * @param context Context from mapper
   * @param configuration Configuration
   * @param service Centralized service
   */
  public NettyMasterClient(Mapper<?, ?, ?, ?>.Context context,
                           ImmutableClassesGiraphConfiguration configuration,
                           CentralizedServiceMaster<?, ?, ?> service) {
    this.nettyClient =
        new NettyClient(context, configuration, service.getMasterInfo());
    this.service = service;
    this.progressable = context;
    maxBytesPerAggregatorRequest = configuration.getInt(
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST,
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST_DEFAULT);
  }

  @Override
  public void openConnections() {
    nettyClient.connectAllAddresses(service.getWorkerInfoList());
  }

  @Override
  public void sendAggregator(String aggregatorName,
      Class<? extends Aggregator> aggregatorClass,
      Writable aggregatedValue) throws IOException {
    WorkerInfo owner =
        AggregatorUtils.getOwner(aggregatorName, service.getWorkerInfoList());
    int currentSize = sendAggregatorCache.addAggregator(owner.getTaskId(),
        aggregatorName, aggregatorClass, aggregatedValue);
    if (currentSize >= maxBytesPerAggregatorRequest) {
      flushAggregatorsToWorker(owner);
    }
  }

  @Override
  public void finishSendingAggregatedValues() throws IOException {
    for (WorkerInfo worker : service.getWorkerInfoList()) {
      sendAggregatorCache.addCountAggregator(worker.getTaskId());
      flushAggregatorsToWorker(worker);
      progressable.progress();
    }
    sendAggregatorCache.reset();
  }

  /**
   * Send aggregators from cache to worker.
   *
   * @param worker Worker which we want to send aggregators to
   */
  private void flushAggregatorsToWorker(WorkerInfo worker) {
    byte[] aggregatorData =
        sendAggregatorCache.removeAggregators(worker.getTaskId());
    nettyClient.sendWritableRequest(
        worker.getTaskId(), new SendAggregatorsToOwnerRequest(aggregatorData,
          service.getMasterInfo().getTaskId()));
  }

  @Override
  public void flush() {
    nettyClient.waitAllRequests();
  }

  @Override
  public void closeConnections() {
    nettyClient.stop();
  }
}
