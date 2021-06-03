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

import java.io.IOException;

import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.comm.aggregators.SendGlobalCommCache;
import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.comm.requests.SendAggregatorsToOwnerRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Progressable;

/**
 * Netty implementation of {@link MasterClient}
 */
public class NettyMasterClient implements MasterClient {
  /** Netty client that does the actual I/O */
  private final NettyClient nettyClient;
  /** Worker information for current superstep */
  private final CentralizedServiceMaster<?, ?, ?> service;
  /** Cached map of partition ids to serialized aggregator data */
  private final SendGlobalCommCache sendGlobalCommCache =
      new SendGlobalCommCache(true);
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
   * @param exceptionHandler handler for uncaught exception. Will
   *                         terminate job.
   */
  public NettyMasterClient(Mapper<?, ?, ?, ?>.Context context,
                           ImmutableClassesGiraphConfiguration configuration,
                           CentralizedServiceMaster<?, ?, ?> service,
                           Thread.UncaughtExceptionHandler exceptionHandler) {
    this.nettyClient =
        new NettyClient(context, configuration, service.getMasterInfo(),
            exceptionHandler);
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
  public void sendToOwner(String name, GlobalCommType sendType, Writable object)
    throws IOException {
    WorkerInfo owner =
        AggregatorUtils.getOwner(name, service.getWorkerInfoList());
    int currentSize = sendGlobalCommCache.addValue(owner.getTaskId(),
        name, sendType, object);
    if (currentSize >= maxBytesPerAggregatorRequest) {
      flushAggregatorsToWorker(owner);
    }
  }

  @Override
  public void finishSendingValues() throws IOException {
    for (WorkerInfo worker : service.getWorkerInfoList()) {
      sendGlobalCommCache.addSpecialCount(worker.getTaskId());
      flushAggregatorsToWorker(worker);
      progressable.progress();
    }
    sendGlobalCommCache.reset();
  }

  /**
   * Send aggregators from cache to worker.
   *
   * @param worker Worker which we want to send aggregators to
   */
  private void flushAggregatorsToWorker(WorkerInfo worker) {
    byte[] data =
        sendGlobalCommCache.removeSerialized(worker.getTaskId());
    nettyClient.sendWritableRequest(
        worker.getTaskId(), new SendAggregatorsToOwnerRequest(data,
          service.getMasterInfo().getTaskId()));
  }

  @Override
  public void flush() {
    nettyClient.waitAllRequests();
  }

  @Override
  public void sendWritableRequest(int destTaskId, WritableRequest request) {
    nettyClient.sendWritableRequest(destTaskId, request);
  }

  @Override
  public void closeConnections() {
    nettyClient.stop();
  }

  @Override
  public FlowControl getFlowControl() {
    return nettyClient.getFlowControl();
  }
}
