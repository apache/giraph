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

import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

/**
 * Test all the netty failure scenarios
 */
@SuppressWarnings("unchecked")
public class RequestFailureTest {
  /** Configuration */
  private ImmutableClassesGiraphConfiguration conf;
  /** Server data */
  private ServerData<IntWritable, IntWritable, IntWritable>
  serverData;
  /** Server */
  private NettyServer server;
  /** Client */
  private NettyClient client;
  /** Mock context */
  private Context context;

  @Before
  public void setUp() throws IOException {
    // Setup the conf
    GiraphConfiguration tmpConf = new GiraphConfiguration();
    tmpConf.setComputationClass(IntNoOpComputation.class);
    conf = new ImmutableClassesGiraphConfiguration(tmpConf);

    context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);
    Counter counter = mock(Counter.class);
    when(context.getCounter(any(String.class), any(String.class))).thenReturn(
        counter);
  }

  private WritableRequest getRequest() {
    // Data to send
    final int partitionId = 0;
    PairList<Integer, VertexIdMessages<IntWritable,
                IntWritable>>
        dataToSend = new PairList<Integer,
        VertexIdMessages<IntWritable, IntWritable>>();
    dataToSend.initialize();
    ByteArrayVertexIdMessages<IntWritable,
            IntWritable> vertexIdMessages =
        new ByteArrayVertexIdMessages<IntWritable, IntWritable>(
            new TestMessageValueFactory<IntWritable>(IntWritable.class));
    vertexIdMessages.setConf(conf);
    vertexIdMessages.initialize();
    dataToSend.add(partitionId, vertexIdMessages);
    for (int i = 1; i < 7; ++i) {
      IntWritable vertexId = new IntWritable(i);
      for (int j = 0; j < i; ++j) {
        vertexIdMessages.add(vertexId, new IntWritable(j));
      }
    }

    // Send the request
    SendWorkerMessagesRequest<IntWritable, IntWritable> request =
        new SendWorkerMessagesRequest<IntWritable, IntWritable>(dataToSend);
    request.setConf(conf);
    return request;
  }

  private void checkResult(int numRequests) {
    // Check the output
    Iterable<IntWritable> vertices =
        serverData.getIncomingMessageStore().getPartitionDestinationVertices(0);
    int keySum = 0;
    int messageSum = 0;
    for (IntWritable vertexId : vertices) {
      keySum += vertexId.get();
      Iterable<IntWritable> messages =
          serverData.<IntWritable>getIncomingMessageStore().getVertexMessages(
              vertexId);
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
    checkSendingTwoRequests();
  }

  @Test
  public void alreadyProcessedRequest() throws IOException {
    // Force a drop of the first request
    GiraphConstants.NETTY_SIMULATE_FIRST_RESPONSE_FAILED.set(conf, true);
    // One second to finish a request
    GiraphConstants.MAX_REQUEST_MILLISECONDS.set(conf, 1000);
    // Loop every 2 seconds
    GiraphConstants.WAITING_REQUEST_MSECS.set(conf, 2000);

    checkSendingTwoRequests();
  }

  @Test
  public void resendRequest() throws IOException {
    // Force a drop of the first request
    GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED.set(conf, false);
    // One second to finish a request
    GiraphConstants.MAX_REQUEST_MILLISECONDS.set(conf, 1000);
    // Loop every 2 seconds
    GiraphConstants.WAITING_REQUEST_MSECS.set(conf, 2000);

    checkSendingTwoRequests();
  }

  private void checkSendingTwoRequests() throws IOException {
    // Start the service
    serverData = MockUtils.createNewServerData(conf, context);
    serverData.prepareSuperstep();
    WorkerInfo workerInfo = new WorkerInfo();
    server = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory(serverData), workerInfo,
            context, new MockExceptionHandler());
    server.start();
    workerInfo.setInetSocketAddress(server.getMyAddress(), server.getLocalHostOrIp());
    client = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    server.setFlowControl(client.getFlowControl());
    client.connectAllAddresses(
        Lists.<WorkerInfo>newArrayList(workerInfo));

    // Send the request 2x, but should only be processed once
    WritableRequest request1 = getRequest();
    WritableRequest request2 = getRequest();
    client.sendWritableRequest(workerInfo.getTaskId(), request1);
    client.sendWritableRequest(workerInfo.getTaskId(), request2);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output (should have been only processed once)
    checkResult(2);
  }
}
