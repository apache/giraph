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
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test all the different netty requests.
 */
public class RequestTest {
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

    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    // Start the service
    serverData =
        new ServerData<IntWritable, IntWritable, IntWritable, IntWritable>
            (SimpleMessageStore.newFactory(
                MockUtils.mockServiceGetVertexPartitionOwner(1), conf));
    server =
        new NettyServer<IntWritable, IntWritable, IntWritable, IntWritable>(
            conf, serverData);
    server.start();
    client =
        new NettyClient<IntWritable, IntWritable, IntWritable, IntWritable>
            (context);
    client.connectAllAddresses(Collections.singleton(server.getMyAddress()));
  }

  @Test
  public void sendVertexPartition() throws IOException {
    // Data to send
    int partitionId = 13;
    Collection<Vertex<IntWritable, IntWritable, IntWritable,
        IntWritable>> vertices =
        new ArrayList<Vertex<IntWritable, IntWritable,
        IntWritable, IntWritable>>();
    for (int i = 0; i < 10; ++i) {
      TestVertex vertex = new TestVertex();
      vertex.initialize(new IntWritable(i), new IntWritable(i), null, null);
      vertices.add(vertex);
    }

    // Send the request
    SendVertexRequest<IntWritable, IntWritable, IntWritable,
    IntWritable> request =
      new SendVertexRequest<IntWritable, IntWritable,
      IntWritable, IntWritable>(partitionId, vertices);
    client.sendWritableRequest(server.getMyAddress(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output
    Map<Integer, Collection<Vertex<IntWritable, IntWritable,
    IntWritable, IntWritable>>> partitionVertexMap =
        serverData.getPartitionVertexMap();
    synchronized (partitionVertexMap) {
      assertTrue(partitionVertexMap.containsKey(partitionId));
      int total = 0;
      for (Vertex<IntWritable, IntWritable,
          IntWritable, IntWritable> vertex :
            (partitionVertexMap.get(partitionId))) {
        total += vertex.getId().get();
      }
      assertEquals(total, 45);
    }
  }

  @Test
  public void sendPartitionMessagesRequest() throws IOException {
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
    client.sendWritableRequest(server.getMyAddress(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

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
    assertEquals(35, messageSum);
  }

  @Test
  public void sendPartitionMutationsRequest() throws IOException {
    // Data to send
    int partitionId = 19;
    Map<IntWritable, VertexMutations<IntWritable, IntWritable,
    IntWritable, IntWritable>> vertexIdMutations =
        Maps.newHashMap();
    for (int i = 0; i < 11; ++i) {
      VertexMutations<IntWritable, IntWritable, IntWritable, IntWritable>
      mutations =
      new VertexMutations<IntWritable, IntWritable,
      IntWritable, IntWritable>();
      for (int j = 0; j < 3; ++j) {
        TestVertex vertex = new TestVertex();
        vertex.initialize(new IntWritable(i), new IntWritable(j), null, null);
        mutations.addVertex(vertex);
      }
      for (int j = 0; j < 2; ++j) {
        mutations.removeVertex();
      }
      for (int j = 0; j < 5; ++j) {
        Edge<IntWritable, IntWritable> edge =
            new Edge<IntWritable, IntWritable>(
                new IntWritable(i), new IntWritable(2*j));
        mutations.addEdge(edge);
      }
      for (int j = 0; j < 7; ++j) {
        mutations.removeEdge(new IntWritable(j));
      }
      vertexIdMutations.put(new IntWritable(i), mutations);
    }

    // Send the request
    SendPartitionMutationsRequest<IntWritable, IntWritable, IntWritable,
    IntWritable> request =
      new SendPartitionMutationsRequest<IntWritable, IntWritable,
      IntWritable, IntWritable>(partitionId, vertexIdMutations);
    client.sendWritableRequest(server.getMyAddress(), request);
    client.waitAllRequests();

    // Stop the service
    client.stop();
    server.stop();

    // Check the output
    ConcurrentHashMap<IntWritable, VertexMutations<IntWritable, IntWritable,
    IntWritable, IntWritable>> inVertexIdMutations =
        serverData.getVertexMutations();
    int keySum = 0;
    for (Entry<IntWritable, VertexMutations<IntWritable, IntWritable,
        IntWritable, IntWritable>> entry :
          inVertexIdMutations.entrySet()) {
      synchronized (entry.getValue()) {
        keySum += entry.getKey().get();
        int vertexValueSum = 0;
        for (Vertex<IntWritable, IntWritable, IntWritable, IntWritable>
        vertex : entry.getValue().getAddedVertexList()) {
          vertexValueSum += vertex.getValue().get();
        }
        assertEquals(3, vertexValueSum);
        assertEquals(2, entry.getValue().getRemovedVertexCount());
        int removeEdgeValueSum = 0;
        for (Edge<IntWritable, IntWritable> edge :
          entry.getValue().getAddedEdgeList()) {
          removeEdgeValueSum += edge.getValue().get();
        }
        assertEquals(20, removeEdgeValueSum);
      }
    }
    assertEquals(55, keySum);
  }
}
