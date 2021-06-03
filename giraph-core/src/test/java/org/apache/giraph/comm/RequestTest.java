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
import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.comm.requests.SendPartitionMutationsRequest;
import org.apache.giraph.comm.requests.SendVertexRequest;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.SendWorkerOneMessageToManyRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayOneMessageToManyIds;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test all the different netty requests.
 */
@SuppressWarnings("unchecked")
public class RequestTest {
  /** Configuration */
  private ImmutableClassesGiraphConfiguration conf;
  /** Server data */
  private ServerData<IntWritable, IntWritable, IntWritable> serverData;
  /** Server */
  private NettyServer server;
  /** Client */
  private NettyClient client;
  /** Worker info */
  private WorkerInfo workerInfo;

  @Before
  public void setUp() {
    // Setup the conf
    GiraphConfiguration tmpConf = new GiraphConfiguration();
    GiraphConstants.COMPUTATION_CLASS.set(tmpConf, IntNoOpComputation.class);
    conf = new ImmutableClassesGiraphConfiguration(tmpConf);

    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    // Start the service
    serverData = MockUtils.createNewServerData(conf, context);
    serverData.prepareSuperstep();
    workerInfo = new WorkerInfo();
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
  }

  @Test
  public void sendVertexPartition() {
    // Data to send
    int partitionId = 13;
    Partition<IntWritable, IntWritable, IntWritable> partition =
        conf.createPartition(partitionId, null);
    for (int i = 0; i < 10; ++i) {
      Vertex vertex = conf.createVertex();
      vertex.initialize(new IntWritable(i), new IntWritable(i));
      partition.putVertex(vertex);
    }

    // Send the request
    SendVertexRequest<IntWritable, IntWritable, IntWritable> request =
      new SendVertexRequest<IntWritable, IntWritable, IntWritable>(partition);
    client.sendWritableRequest(workerInfo.getTaskId(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output
    PartitionStore<IntWritable, IntWritable, IntWritable> partitionStore =
        serverData.getPartitionStore();
    assertTrue(partitionStore.hasPartition(partitionId));
    int total = 0;
    Partition<IntWritable, IntWritable, IntWritable> partition2 =
        partitionStore.removePartition(partitionId);
    for (Vertex<IntWritable, IntWritable, IntWritable> vertex : partition2) {
      total += vertex.getId().get();
    }
    partitionStore.putPartition(partition2);
    assertEquals(total, 45);
    partitionStore.shutdown();
  }

  @Test
  public void sendWorkerMessagesRequest() {
    // Data to send
    PairList<Integer, VertexIdMessages<IntWritable,
            IntWritable>>
        dataToSend = new PairList<>();
    dataToSend.initialize();
    int partitionId = 0;
    ByteArrayVertexIdMessages<IntWritable,
            IntWritable> vertexIdMessages =
        new ByteArrayVertexIdMessages<>(
            new TestMessageValueFactory<>(IntWritable.class));
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
      new SendWorkerMessagesRequest<>(dataToSend);
    request.setConf(conf);

    client.sendWritableRequest(workerInfo.getTaskId(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

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
    assertEquals(35, messageSum);
  }

  @Test
  public void sendWorkerIndividualMessagesRequest() throws IOException {
    // Data to send
    ByteArrayOneMessageToManyIds<IntWritable, IntWritable>
        dataToSend = new ByteArrayOneMessageToManyIds<>(new
        TestMessageValueFactory<>(IntWritable.class));
    dataToSend.setConf(conf);
    dataToSend.initialize();
    ExtendedDataOutput output = conf.createExtendedDataOutput();
    for (int i = 1; i <= 7; ++i) {
      IntWritable vertexId = new IntWritable(i);
      vertexId.write(output);
    }
    dataToSend.add(output.getByteArray(), output.getPos(), 7, new IntWritable(1));

    // Send the request
    SendWorkerOneMessageToManyRequest<IntWritable, IntWritable> request =
      new SendWorkerOneMessageToManyRequest<>(dataToSend, conf);
    client.sendWritableRequest(workerInfo.getTaskId(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

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
    assertEquals(28, keySum);
    assertEquals(7, messageSum);
  }

  @Test
  public void sendPartitionMutationsRequest() {
    // Data to send
    int partitionId = 19;
    Map<IntWritable, VertexMutations<IntWritable, IntWritable,
    IntWritable>> vertexIdMutations =
        Maps.newHashMap();
    for (int i = 0; i < 11; ++i) {
      VertexMutations<IntWritable, IntWritable, IntWritable> mutations =
          new VertexMutations<IntWritable, IntWritable, IntWritable>();
      for (int j = 0; j < 3; ++j) {
        Vertex vertex = conf.createVertex();
        vertex.initialize(new IntWritable(i), new IntWritable(j));
        mutations.addVertex(vertex);
      }
      for (int j = 0; j < 2; ++j) {
        mutations.removeVertex();
      }
      for (int j = 0; j < 5; ++j) {
        Edge<IntWritable, IntWritable> edge =
            EdgeFactory.create(new IntWritable(i), new IntWritable(2 * j));
        mutations.addEdge(edge);
      }
      for (int j = 0; j < 7; ++j) {
        mutations.removeEdge(new IntWritable(j));
      }
      vertexIdMutations.put(new IntWritable(i), mutations);
    }

    // Send the request
    SendPartitionMutationsRequest<IntWritable, IntWritable, IntWritable>
        request = new SendPartitionMutationsRequest<IntWritable, IntWritable,
        IntWritable>(partitionId,
        vertexIdMutations);
    GiraphMetrics.init(conf);
    client.sendWritableRequest(workerInfo.getTaskId(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output
    ConcurrentMap<IntWritable,
        VertexMutations<IntWritable, IntWritable, IntWritable>>
        inVertexIdMutations =
        serverData.getPartitionMutations().get(partitionId);
    int keySum = 0;
    for (Entry<IntWritable,
        VertexMutations<IntWritable, IntWritable, IntWritable>> entry :
        inVertexIdMutations
        .entrySet()) {
      synchronized (entry.getValue()) {
        keySum += entry.getKey().get();
        int vertexValueSum = 0;
        for (Vertex<IntWritable, IntWritable, IntWritable> vertex : entry
            .getValue().getAddedVertexList()) {
          vertexValueSum += vertex.getValue().get();
        }
        assertEquals(3, vertexValueSum);
        assertEquals(2, entry.getValue().getRemovedVertexCount());
        int removeEdgeValueSum = 0;
        for (Edge<IntWritable, IntWritable> edge : entry.getValue()
            .getAddedEdgeList()) {
          removeEdgeValueSum += edge.getValue().get();
        }
        assertEquals(20, removeEdgeValueSum);
      }
    }
    assertEquals(55, keySum);
  }
}