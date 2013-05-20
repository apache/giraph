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
import org.apache.giraph.comm.netty.handler.MasterRequestServerHandler;
import org.apache.giraph.comm.MasterServer;
import org.apache.hadoop.util.Progressable;

import java.net.InetSocketAddress;

/**
 * Netty implementation of {@link MasterServer}
 */
public class NettyMasterServer implements MasterServer {
  /** Netty client that does the actual I/O */
  private final NettyServer nettyServer;

  /**
   * Constructor
   *
   * @param conf Hadoop configuration
   * @param service Centralized service
   * @param progressable Progressable for reporting progress
   */
  public NettyMasterServer(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceMaster<?, ?, ?> service,
      Progressable progressable) {
    nettyServer = new NettyServer(conf,
        new MasterRequestServerHandler.Factory(service.getAggregatorHandler()),
        service.getMasterInfo(), progressable);
    nettyServer.start();
  }

  @Override
  public InetSocketAddress getMyAddress() {
    return nettyServer.getMyAddress();
  }

  @Override
  public void close() {
    nettyServer.stop();
  }
}
