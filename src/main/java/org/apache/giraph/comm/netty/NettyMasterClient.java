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

import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;

/**
 * Netty implementation of {@link MasterClient}
 */
public class NettyMasterClient implements MasterClient {
  /** Netty client that does the actual I/O */
  private final NettyClient nettyClient;
  /** Worker information for current superstep */
  private Collection<WorkerInfo> workers;

  /**
   * Constructor
   *
   * @param context Context from mapper
   */
  public NettyMasterClient(Mapper<?, ?, ?, ?>.Context context) {
    this.nettyClient = new NettyClient(context);
    workers = Lists.newArrayList();
  }

  @Override
  public void fixWorkerAddresses(Iterable<WorkerInfo> workers) {
    this.workers.clear();
    Iterables.addAll(this.workers, workers);
    Set<InetSocketAddress> addresses =
        Sets.newHashSetWithExpectedSize(this.workers.size());
    for (WorkerInfo worker : workers) {
      addresses.add(worker.getInetSocketAddress());
    }
    nettyClient.connectAllAddresses(addresses);
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
