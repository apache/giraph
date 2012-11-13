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

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Takes users facing APIs in {@link WorkerClient} and implements them
 * using the available {@link WritableRequest} objects.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerClient<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> implements
    WorkerClient<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyWorkerClient.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Netty client that does that actual I/O */
  private final NettyClient nettyClient;
  /** Centralized service, needed to get vertex ranges */
  private final CentralizedServiceWorker<I, V, E, M> service;

  /**
   * Only constructor.
   *
   * @param context Context from mapper
   * @param configuration Configuration
   * @param service Used to get partition mapping
   */
  public NettyWorkerClient(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      CentralizedServiceWorker<I, V, E, M> service) {
    this.nettyClient =
        new NettyClient(context, configuration, service.getWorkerInfo());
    this.conf = configuration;
    this.service = service;
  }

  public CentralizedServiceWorker<I, V, E, M> getService() {
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
  public void sendWritableRequest(Integer destTaskId,
                                  WritableRequest request) {
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
}
