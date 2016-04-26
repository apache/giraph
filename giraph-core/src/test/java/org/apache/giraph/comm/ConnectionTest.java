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

package org.apache.giraph.comm;

import com.google.common.collect.Lists;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.handler.RequestServerHandler;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the netty connections
 */
public class ConnectionTest {
  /** Class configuration */
  private ImmutableClassesGiraphConfiguration conf;

  @Before
  public void setUp() {
    GiraphConfiguration tmpConfig = new GiraphConfiguration();
    tmpConfig.setComputationClass(IntNoOpComputation.class);
    conf = new ImmutableClassesGiraphConfiguration(tmpConfig);
  }

  /**
   * Test connecting a single client to a single server.
   *
   * @throws IOException
   */
  @Test
  public void connectSingleClientServer() throws IOException {
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
    WorkerInfo workerInfo = new WorkerInfo();
    NettyServer server =
        new NettyServer(conf,
            new WorkerRequestServerHandler.Factory(serverData), workerInfo,
            context, new MockExceptionHandler());
    server.start();
    workerInfo.setInetSocketAddress(server.getMyAddress(), server.getLocalHostOrIp());

    NettyClient client = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    server.setFlowControl(client.getFlowControl());
    client.connectAllAddresses(
        Lists.<WorkerInfo>newArrayList(workerInfo));

    client.stop();
    server.stop();
  }

  /**
   * Test connecting one client to three servers.
   *
   * @throws IOException
   */
  @Test
  public void connectOneClientToThreeServers() throws IOException {
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
   RequestServerHandler.Factory requestServerHandlerFactory =
       new WorkerRequestServerHandler.Factory(serverData);

    WorkerInfo workerInfo1 = new WorkerInfo();
    workerInfo1.setTaskId(1);
    NettyServer server1 =
        new NettyServer(conf, requestServerHandlerFactory, workerInfo1,
            context, new MockExceptionHandler());
    server1.start();
    workerInfo1.setInetSocketAddress(server1.getMyAddress(), server1.getLocalHostOrIp());

    WorkerInfo workerInfo2 = new WorkerInfo();
    workerInfo1.setTaskId(2);
    NettyServer server2 =
        new NettyServer(conf, requestServerHandlerFactory, workerInfo2,
            context, new MockExceptionHandler());
    server2.start();
    workerInfo2.setInetSocketAddress(server2.getMyAddress(), server1.getLocalHostOrIp());

    WorkerInfo workerInfo3 = new WorkerInfo();
    workerInfo1.setTaskId(3);
    NettyServer server3 =
        new NettyServer(conf, requestServerHandlerFactory, workerInfo3,
            context, new MockExceptionHandler());
    server3.start();
    workerInfo3.setInetSocketAddress(server3.getMyAddress(), server1.getLocalHostOrIp());

    NettyClient client = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    server1.setFlowControl(client.getFlowControl());
    server2.setFlowControl(client.getFlowControl());
    server3.setFlowControl(client.getFlowControl());
    List<WorkerInfo> addresses = Lists.<WorkerInfo>newArrayList(workerInfo1,
        workerInfo2, workerInfo3);
    client.connectAllAddresses(addresses);

    client.stop();
    server1.stop();
    server2.stop();
    server3.stop();
  }

  /**
   * Test connecting three clients to one server.
   *
   * @throws IOException
   */
  @Test
  public void connectThreeClientsToOneServer() throws IOException {
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
    WorkerInfo workerInfo = new WorkerInfo();
    NettyServer server = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory(serverData), workerInfo,
            context, new MockExceptionHandler());
    server.start();
    workerInfo.setInetSocketAddress(server.getMyAddress(), server.getLocalHostOrIp());

    List<WorkerInfo> addresses = Lists.<WorkerInfo>newArrayList(workerInfo);
    NettyClient client1 = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    client1.connectAllAddresses(addresses);
    NettyClient client2 = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    client2.connectAllAddresses(addresses);
    NettyClient client3 = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    client3.connectAllAddresses(addresses);
    server.setFlowControl(client1.getFlowControl());

    client1.stop();
    client2.stop();
    client3.stop();
    server.stop();
  }
}
