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

import org.apache.giraph.comm.messages.SimpleMessageStore;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jboss.netty.channel.socket.DefaultSocketChannelConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test the netty connections
 */
public class ConnectionTest {
  /**
   * Test connecting a single client to a single server.
   *
   * @throws IOException
   */
  @Test
  public void connectSingleClientServer() throws IOException {
    Configuration conf = new Configuration();
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();

    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client =
        new NettyClient<IntWritable, IntWritable, IntWritable,
        IntWritable>(context);
    client.connectAllAdddresses(Collections.singleton(server.getMyAddress()));

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
    Configuration conf = new Configuration();
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);
    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));

    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server1 =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server1.start();
    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server2 =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server2.start();
    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server3 =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server3.start();

    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client =
        new NettyClient<IntWritable, IntWritable, IntWritable,
        IntWritable>(context);
    List<InetSocketAddress> serverAddresses =
        new ArrayList<InetSocketAddress>();
    client.connectAllAdddresses(serverAddresses);

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
    Configuration conf = new Configuration();
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();

    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client1 =
        new NettyClient<IntWritable, IntWritable, IntWritable,
        IntWritable>(context);
    client1.connectAllAdddresses(Collections.singleton(server.getMyAddress()));
    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client2 =
        new NettyClient<IntWritable, IntWritable, IntWritable,
        IntWritable>(context);
    client2.connectAllAdddresses(Collections.singleton(server.getMyAddress()));
    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client3 =
        new NettyClient<IntWritable, IntWritable, IntWritable,
        IntWritable>(context);
    client3.connectAllAdddresses(Collections.singleton(server.getMyAddress()));

    client1.stop();
    client2.stop();
    client3.stop();
    server.stop();
  }


  /**
   * Test that we can use Giraph configuration settings to
   * modify Netty client channel configuration.
   * TODO: add test for server-side channel configuration as well.
   *
   * @throws IOException
   */
  @Test
  public void testClientChannelConfiguration() throws IOException {
    Configuration conf = new Configuration();
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable, IntWritable> serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));

    NettyServer<IntWritable, IntWritable, IntWritable, IntWritable> server = new NettyServer<IntWritable, IntWritable,
        IntWritable, IntWritable>(conf, serverData);
    server.start();

    final int giraphClientSendBufferSize = context.getConfiguration().getInt(GiraphJob.CLIENT_SEND_BUFFER_SIZE,
        GiraphJob.DEFAULT_CLIENT_SEND_BUFFER_SIZE);
    final int giraphClientReceiveBufferSize = context.getConfiguration().getInt(GiraphJob.CLIENT_RECEIVE_BUFFER_SIZE,
        GiraphJob.DEFAULT_CLIENT_RECEIVE_BUFFER_SIZE);

    NettyClient<IntWritable, IntWritable, IntWritable, IntWritable> client =
        new NettyClient<IntWritable, IntWritable, IntWritable,
            IntWritable>(context);
    client.connectAllAdddresses(Collections.singleton(server.getMyAddress()));

    DefaultSocketChannelConfig clientConfig = (DefaultSocketChannelConfig)client.getChannelConfig();
    final int channelClientSendBufferSize = clientConfig.getSendBufferSize();
    final int channelClientReceiveBufferSize = clientConfig.getReceiveBufferSize();

    assertEquals(giraphClientSendBufferSize,channelClientSendBufferSize);
    assertEquals(giraphClientReceiveBufferSize,channelClientReceiveBufferSize);

    client.stop();
    server.stop();

  }
}
