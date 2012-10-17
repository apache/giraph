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
import org.apache.giraph.comm.MasterClientServer;
import org.apache.giraph.comm.MasterServer;
import org.apache.hadoop.mapreduce.Mapper;

import java.net.InetSocketAddress;

/**
 * Netty based implementation of the {@link MasterClientServer} interface.
 */
public class NettyMasterClientServer implements MasterClientServer {
  /** Client that sends requests */
  private final MasterClient client;
  /** Server that processes requests */
  private final MasterServer server;

  /**
   * Constructor
   *
   * @param context Mapper context
   * @param configuration Configuration
   * @param service Centralized service
   */
  public NettyMasterClientServer(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration configuration,
      CentralizedServiceMaster<?, ?, ?, ?> service) {
    client = new NettyMasterClient(context, configuration, service);
    server = new NettyMasterServer(configuration);
  }

  @Override
  public void openConnections() {
    client.openConnections();
  }

  @Override
  public void flush() {
    client.flush();
  }

  @Override
  public void closeConnections() {
    client.closeConnections();
  }

  @Override
  public InetSocketAddress getMyAddress() {
    return server.getMyAddress();
  }

  @Override
  public void close() {
    server.close();
  }
}
