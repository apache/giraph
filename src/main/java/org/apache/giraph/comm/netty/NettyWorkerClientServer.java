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
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.WorkerClientServer;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Netty based implementation of the {@link WorkerClientServer} interface.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerClientServer<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerClientServer<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(NettyWorkerClientServer.class);
  /** Client that sends requests */
  private final WorkerClient<I, V, E, M> client;
  /** Server that processes requests */
  private final WorkerServer<I, V, E, M> server;

  /**
   * Constructor.
   *
   * @param context Mapper context
   * @param configuration Configuration
   * @param service Service for partition lookup
   */
  public NettyWorkerClientServer(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      CentralizedServiceWorker<I, V, E, M> service) {
    server = new NettyWorkerServer<I, V, E, M>(
        configuration, service, context);
    client = new NettyWorkerClient<I, V, E, M>(context,
        configuration, service, server.getServerData());
  }

  @Override
  public void fixPartitionIdToSocketAddrMap() {
    client.fixPartitionIdToSocketAddrMap();
  }

  @Override
  public void sendMessageRequest(I destVertexId, M message) {
    client.sendMessageRequest(destVertexId, message);
  }

  @Override
  public void sendPartitionRequest(WorkerInfo workerInfo,
                                   Partition<I, V, E, M> partition) {
    client.sendPartitionRequest(workerInfo, partition);
  }

  @Override
  public void addEdgeRequest(I vertexIndex, Edge<I, E> edge) throws
      IOException {
    client.addEdgeRequest(vertexIndex, edge);
  }

  @Override
  public void removeEdgeRequest(I vertexIndex,
                                I destinationVertexIndex) throws IOException {
    client.removeEdgeRequest(vertexIndex, destinationVertexIndex);
  }

  @Override
  public void addVertexRequest(Vertex<I, V, E, M> vertex) throws IOException {
    client.addVertexRequest(vertex);
  }

  @Override
  public void removeVertexRequest(I vertexIndex) throws IOException {
    client.removeVertexRequest(vertexIndex);
  }

  @Override
  public void flush() throws IOException {
    client.flush();
  }

  @Override
  public long resetMessageCount() {
    return client.resetMessageCount();
  }

  @Override
  public void closeConnections() throws IOException {
    client.closeConnections();
  }

  @Override
/*if[HADOOP_NON_SECURE]
  public void setup() {
    client.fixPartitionIdToSocketAddrMap();
  }
else[HADOOP_NON_SECURE]*/
  public void setup(boolean authenticateWithServer) {
    client.fixPartitionIdToSocketAddrMap();
    if (authenticateWithServer) {
      try {
        client.authenticate();
      } catch (IOException e) {
        LOG.error("setup: Failed to authenticate : " + e);
      }
    }
  }
/*end[HADOOP_NON_SECURE]*/

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
  @Override
  public void authenticate() throws IOException {
    client.authenticate();
  }
/*end[HADOOP_NON_SECURE]*/

  @Override
  public void prepareSuperstep() {
    server.prepareSuperstep();
  }

  @Override
  public ServerData<I, V, E, M> getServerData() {
    return server.getServerData();
  }

  @Override
  public void close() {
    server.close();
  }


  @Override
  public int getPort() {
    return server.getPort();
  }
}
