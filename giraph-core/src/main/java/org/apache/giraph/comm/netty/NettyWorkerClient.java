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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Counter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Takes users facing APIs in {@link WorkerClient} and implements them
 * using the available {@link WritableRequest} objects.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerClient<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    WorkerClient<I, V, E>, ResetSuperstepMetricsObserver {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyWorkerClient.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Netty client that does that actual I/O */
  private final NettyClient nettyClient;
  /** Centralized service, needed to get vertex ranges */
  private final CentralizedServiceWorker<I, V, E> service;

  // Metrics
  /** Per-superstep, per-request counters */
  private final Map<RequestType, Counter> superstepRequestCounters;

  /**
   * Only constructor.
   *
   * @param context Context from mapper
   * @param configuration Configuration
   * @param service Used to get partition mapping
   * @param exceptionHandler handler for uncaught exception. Will
   *                         terminate job.
   */
  public NettyWorkerClient(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      CentralizedServiceWorker<I, V, E> service,
      Thread.UncaughtExceptionHandler exceptionHandler) {
    this.nettyClient =
        new NettyClient(context, configuration, service.getWorkerInfo(),
            exceptionHandler);
    this.conf = configuration;
    this.service = service;
    this.superstepRequestCounters = Maps.newHashMap();
    GiraphMetrics.get().addSuperstepResetObserver(this);
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry metrics) {
    superstepRequestCounters.clear();
    superstepRequestCounters.put(RequestType.SEND_WORKER_VERTICES_REQUEST,
        metrics.getCounter(MetricNames.SEND_VERTEX_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_WORKER_MESSAGES_REQUEST,
        metrics.getCounter(MetricNames.SEND_WORKER_MESSAGES_REQUESTS));
    superstepRequestCounters.put(
        RequestType.SEND_PARTITION_CURRENT_MESSAGES_REQUEST,
        metrics.getCounter(
            MetricNames.SEND_PARTITION_CURRENT_MESSAGES_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_PARTITION_MUTATIONS_REQUEST,
        metrics.getCounter(MetricNames.SEND_PARTITION_MUTATIONS_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_WORKER_AGGREGATORS_REQUEST,
        metrics.getCounter(MetricNames.SEND_WORKER_AGGREGATORS_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_AGGREGATORS_TO_MASTER_REQUEST,
        metrics.getCounter(MetricNames.SEND_AGGREGATORS_TO_MASTER_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_AGGREGATORS_TO_OWNER_REQUEST,
        metrics.getCounter(MetricNames.SEND_AGGREGATORS_TO_OWNER_REQUESTS));
    superstepRequestCounters.put(RequestType.SEND_AGGREGATORS_TO_WORKER_REQUEST,
        metrics.getCounter(MetricNames.SEND_AGGREGATORS_TO_WORKER_REQUESTS));
  }

  public CentralizedServiceWorker<I, V, E> getService() {
    return service;
  }

  @Override
  public void openConnections() {
    List<TaskInfo> addresses = Lists.newArrayListWithCapacity(
        service.getWorkerInfoList().size());
    for (WorkerInfo info : service.getWorkerInfoList()) {
      // No need to connect to myself
      if (service.getWorkerInfo().getTaskId() != info.getTaskId()) {
        addresses.add(info);
      }
    }
    addresses.add(service.getMasterInfo());
    nettyClient.connectAllAddresses(addresses);
  }

  @Override
  public PartitionOwner getVertexPartitionOwner(I vertexId) {
    return service.getVertexPartitionOwner(vertexId);
  }

  @Override
  public void sendWritableRequest(int destTaskId,
                                  WritableRequest request) {
    Counter counter = superstepRequestCounters.get(request.getType());
    if (counter != null) {
      counter.inc();
    }
    nettyClient.sendWritableRequest(destTaskId, request);
  }

  @Override
  public void waitAllRequests() {
    nettyClient.waitAllRequests();
  }

  @Override
  public void closeConnections() throws IOException {
    nettyClient.stop();
  }

/*if[HADOOP_NON_SECURE]
  @Override
  public void setup() {
    openConnections();
  }
else[HADOOP_NON_SECURE]*/
  @Override
  public void setup(boolean authenticate) {
    openConnections();
    if (authenticate) {
      authenticate();
    }
  }
/*end[HADOOP_NON_SECURE]*/

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
  @Override
  public void authenticate() {
    nettyClient.authenticate();
  }

/*end[HADOOP_NON_SECURE]*/

  @Override
  public FlowControl getFlowControl() {
    return nettyClient.getFlowControl();
  }
}
