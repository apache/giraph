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
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.comm.MasterClient;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Netty implementation of {@link MasterClient}
 */
public class NettyMasterClient implements MasterClient {
  /** Netty client that does the actual I/O */
  private final NettyClient nettyClient;
  /** Worker information for current superstep */
  private CentralizedServiceMaster<?, ?, ?, ?> service;

  /**
   * Constructor
   *
   * @param context Context from mapper
   * @param configuration Configuration
   * @param service Centralized service
   */
  public NettyMasterClient(Mapper<?, ?, ?, ?>.Context context,
                           ImmutableClassesGiraphConfiguration configuration,
                           CentralizedServiceMaster<?, ?, ?, ?> service) {
    this.nettyClient = new NettyClient(context, configuration);
    this.service = service;
  }

  @Override
  public void openConnections() {
    nettyClient.connectAllAddresses(service.getWorkerInfoList());
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
