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

import java.util.List;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.comm.netty.handler.RequestServerHandler;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

/**
 * Test the netty connections
 */
public class ConnectionTest {
  /** Class configuration */
  private ImmutableClassesGiraphConfiguration conf;

  public static class IntVertex extends EdgeListVertex<IntWritable,
          IntWritable, IntWritable, IntWritable> {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
    }
  }

  @Before
  public void setUp() {
    GiraphConfiguration tmpConfig = new GiraphConfiguration();
    tmpConfig.setVertexClass(IntVertex.class);
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

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
    NettyServer server =
        new NettyServer(conf,
            new WorkerRequestServerHandler.Factory(serverData));
    server.start();

    NettyClient client = new NettyClient(context, conf);
    client.connectAllAddresses(
        Lists.<WorkerInfo>newArrayList(
            new WorkerInfo(server.getMyAddress(), -1)));

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

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
   RequestServerHandler.Factory requestServerHandlerFactory =
       new WorkerRequestServerHandler.Factory(serverData);

    NettyServer server1 = new NettyServer(conf, requestServerHandlerFactory);
    server1.start();
    NettyServer server2 = new NettyServer(conf, requestServerHandlerFactory);
    server2.start();
    NettyServer server3 = new NettyServer(conf, requestServerHandlerFactory);
    server3.start();

    NettyClient client = new NettyClient(context, conf);
    WorkerInfo workerInfo1 = new WorkerInfo(server1.getMyAddress(), 1);
    WorkerInfo workerInfo2 = new WorkerInfo(server2.getMyAddress(), 2);
    WorkerInfo workerInfo3 = new WorkerInfo(server3.getMyAddress(), 3);
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

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);
    NettyServer server = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory(serverData));
    server.start();

    List<WorkerInfo> addresses =
        Lists.<WorkerInfo>newArrayList(
            new WorkerInfo(server.getMyAddress(), -1));
    NettyClient client1 = new NettyClient(context, conf);
    client1.connectAllAddresses(addresses);
    NettyClient client2 = new NettyClient(context, conf);
    client2.connectAllAddresses(addresses);
    NettyClient client3 = new NettyClient(context, conf);
    client3.connectAllAddresses(addresses);

    client1.stop();
    client2.stop();
    client3.stop();
    server.stop();
  }
}
