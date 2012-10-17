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

import java.util.concurrent.ConcurrentMap;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
  /** Signal for getInetSocketAddress() to use WorkerInfo's address */
  public static final int NO_PARTITION_ID = Integer.MIN_VALUE;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyWorkerClient.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Netty client that does that actual I/O */
  private final NettyClient nettyClient;
  /** Centralized service, needed to get vertex ranges */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /**
   * Cached map of partition ids to remote socket address.
   */
  private final ConcurrentMap<Integer, InetSocketAddress>
  partitionIndexAddressMap =
      new ConcurrentHashMap<Integer, InetSocketAddress>();
  /** Maximum number of attempts to resolve an address*/
  private final int maxResolveAddressAttempts;

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
    this.nettyClient = new NettyClient(context, configuration);
    this.conf = configuration;
    this.service = service;

    maxResolveAddressAttempts = conf.getInt(
        GiraphConfiguration.MAX_RESOLVE_ADDRESS_ATTEMPTS,
        GiraphConfiguration.MAX_RESOLVE_ADDRESS_ATTEMPTS_DEFAULT);
  }

  public CentralizedServiceWorker<I, V, E, M> getService() {
    return service;
  }

  @Override
  public void openConnections(
      Collection<? extends PartitionOwner> partitionOwners) {
    // 1. Fix all the cached inet addresses (remove all changed entries)
    // 2. Connect to any new IPC servers
    Map<InetSocketAddress, Integer> addressTaskIdMap =
        Maps.newHashMapWithExpectedSize(partitionOwners.size());
    for (PartitionOwner partitionOwner : partitionOwners) {
      InetSocketAddress address =
          partitionIndexAddressMap.get(
              partitionOwner.getPartitionId());
      if (address != null &&
          (!address.getHostName().equals(
              partitionOwner.getWorkerInfo().getHostname()) ||
              address.getPort() !=
              partitionOwner.getWorkerInfo().getPort())) {
        if (LOG.isInfoEnabled()) {
          LOG.info("fixPartitionIdToSocketAddrMap: " +
              "Partition owner " +
              partitionOwner + " changed from " +
              address);
        }
        partitionIndexAddressMap.remove(
            partitionOwner.getPartitionId());
      }

      // No need to connect to myself
      if (service.getWorkerInfo().getTaskId() !=
          partitionOwner.getWorkerInfo().getTaskId()) {
        addressTaskIdMap.put(
            getInetSocketAddress(partitionOwner.getWorkerInfo(),
                partitionOwner.getPartitionId()),
            partitionOwner.getWorkerInfo().getTaskId());
      }
    }

    addressTaskIdMap.put(service.getMasterInfo().getInetSocketAddress(),
        null);
    nettyClient.connectAllAddresses(addressTaskIdMap);
  }

  @Override
  public InetSocketAddress getInetSocketAddress(WorkerInfo workerInfo,
      int partitionId) {
    InetSocketAddress address = partitionIndexAddressMap.get(partitionId);
    if (address == null) {
      address = resolveAddress(maxResolveAddressAttempts,
          workerInfo.getInetSocketAddress());
      if (partitionId != NO_PARTITION_ID) {
        // Only cache valid partition ids
        partitionIndexAddressMap.put(partitionId, address);
      }
    }

    return address;
  }

  /**
   * Utility method for getInetSocketAddress()
   *
   * @param maxResolveAddressAttempts Maximum number of attempts to resolve the
   *        address
   * @param address the address we are attempting to resolve
   * @return the successfully resolved address.
   * @throws IllegalStateException if the address is not resolved
   *         in <code>maxResolveAddressAttempts</code> tries.
   */
  private static InetSocketAddress resolveAddress(
      int maxResolveAddressAttempts, InetSocketAddress address) {
    int resolveAttempts = 0;
    while (address.isUnresolved() &&
      resolveAttempts < maxResolveAddressAttempts) {
      ++resolveAttempts;
      LOG.warn("resolveAddress: Failed to resolve " + address +
        " on attempt " + resolveAttempts + " of " +
        maxResolveAddressAttempts + " attempts, sleeping for 5 seconds");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn("resolveAddress: Interrupted.", e);
      }
      address = new InetSocketAddress(address.getHostName(),
          address.getPort());
    }
    if (resolveAttempts >= maxResolveAddressAttempts) {
      throw new IllegalStateException("resolveAddress: Couldn't " +
        "resolve " + address + " in " +  resolveAttempts + " tries.");
    }
    return address;
  }

  @Override
  public PartitionOwner getVertexPartitionOwner(I vertexId) {
    return service.getVertexPartitionOwner(vertexId);
  }

  @Override
  public void sendWritableRequest(Integer destWorkerId,
                                  InetSocketAddress remoteServer,
                                  WritableRequest request) {
    nettyClient.sendWritableRequest(destWorkerId, remoteServer, request);
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
    openConnections(service.getPartitionOwners());
  }
else[HADOOP_NON_SECURE]*/
  @Override
  public void setup(boolean authenticate) {
    openConnections(service.getPartitionOwners());
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
