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
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Test all the netty failure scenarios
 */
public class RequestFailureTest {
  /** Configuration */
  private Configuration conf;
  /** Server data */
  private ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
  serverData;
  /** Server */
  private NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>
  server;
  /** Client */
  private NettyClient<IntWritable, IntWritable, IntWritable, IntWritable>
  client;
  /** Mock context */
  private Context context;

  /**
   * Only for testing.
   */
  public static class TestVertex extends EdgeListVertex<IntWritable,
      IntWritable, IntWritable, IntWritable> {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
    }
  }

  @Before
  public void setUp() throws IOException {
    // Setup the conf
    conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_CLASS, TestVertex.class, Vertex.class);
    conf.setClass(GiraphJob.VERTEX_ID_CLASS,
        IntWritable.class, WritableComparable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS,
        IntWritable.class, Writable.class);
    conf.setClass(GiraphJob.EDGE_VALUE_CLASS,
        IntWritable.class, Writable.class);
    conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS,
        IntWritable.class, Writable.class);

    context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);
  }

  private WritableRequest<IntWritable, IntWritable, IntWritable,
      IntWritable> getRequest() {
    // Data to send
    int partitionId = 17;
    Map<IntWritable, Collection<IntWritable>> vertexIdMessages =
        Maps.newHashMap();
    for (int i = 1; i < 7; ++i) {
      IntWritable vertexId = new IntWritable(i);
      Collection<IntWritable> messages = Lists.newArrayList();
      for (int j = 0; j < i; ++j) {
        messages.add(new IntWritable(j));
      }
      vertexIdMessages.put(vertexId, messages);
    }

    // Send the request
    SendPartitionMessagesRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request =
        new SendPartitionMessagesRequest<IntWritable, IntWritable,
            IntWritable, IntWritable>(partitionId, vertexIdMessages);
    return request;
  }

  private void checkResult(int numRequests) throws IOException {
    // Check the output
    Iterable<IntWritable> vertices =
        serverData.getIncomingMessageStore().getDestinationVertices();
    int keySum = 0;
    int messageSum = 0;
    for (IntWritable vertexId : vertices) {
      keySum += vertexId.get();
      Collection<IntWritable> messages =
          serverData.getIncomingMessageStore().getVertexMessages(vertexId);
      synchronized (messages) {
        for (IntWritable message : messages) {
          messageSum += message.get();
        }
      }
    }
    assertEquals(21, keySum);
    assertEquals(35 * numRequests, messageSum);
  }

  @Test
  public void send2Requests() throws IOException {
    // Start the service
    serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (conf, SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();
    client =
        new NettyClient<IntWritable, IntWritable, IntWritable, IntWritable>
            (context);
    client.connectAllAddresses(Collections.singleton(server.getMyAddress()));

    // Send the request 2x
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request1 = getRequest();
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request2 = getRequest();
    client.sendWritableRequest(-1, server.getMyAddress(), request1);
    client.sendWritableRequest(-1, server.getMyAddress(), request2);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output (should have been only processed once)
    checkResult(2);
  }

  @Test
  public void alreadyProcessedRequest() throws IOException {
    // Force a drop of the first request
    conf.setBoolean(GiraphJob.NETTY_SIMULATE_FIRST_RESPONSE_FAILED, true);
    // One second to finish a request
    conf.setInt(GiraphJob.MAX_REQUEST_MILLISECONDS, 1000);
    // Loop every 2 seconds
    conf.setInt(GiraphJob.WAITING_REQUEST_MSECS, 2000);

    // Start the service
    serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (conf, SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();
    client =
        new NettyClient<IntWritable, IntWritable, IntWritable, IntWritable>
            (context);
    client.connectAllAddresses(Collections.singleton(server.getMyAddress()));

    // Send the request 2x, but should only be processed once
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request1 = getRequest();
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request2 = getRequest();
    client.sendWritableRequest(-1, server.getMyAddress(), request1);
    client.sendWritableRequest(-1, server.getMyAddress(), request2);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output (should have been only processed once)
    checkResult(2);
  }

  @Test
  public void resendRequest() throws IOException {
    // Force a drop of the first request
    conf.setBoolean(GiraphJob.NETTY_SIMULATE_FIRST_REQUEST_CLOSED, true);
    // One second to finish a request
    conf.setInt(GiraphJob.MAX_REQUEST_MILLISECONDS, 1000);
    // Loop every 2 seconds
    conf.setInt(GiraphJob.WAITING_REQUEST_MSECS, 2000);

    // Start the service
    serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (conf, SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();
    client =
        new NettyClient<IntWritable, IntWritable, IntWritable, IntWritable>
            (context);
    client.connectAllAddresses(Collections.singleton(server.getMyAddress()));

    // Send the request 2x, but should only be processed once
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request1 = getRequest();
    WritableRequest<IntWritable, IntWritable, IntWritable,
        IntWritable> request2 = getRequest();
    client.sendWritableRequest(-1, server.getMyAddress(), request1);
    client.sendWritableRequest(-1, server.getMyAddress(), request2);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output (should have been only processed once)
    checkResult(2);
  }
}
