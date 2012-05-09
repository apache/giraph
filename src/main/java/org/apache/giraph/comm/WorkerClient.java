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

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Public interface for workers to do message communication
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public interface WorkerClient<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /**
   *  Setup the client.
   */
  void setup();

  /**
   * Fix changes to the workers and the mapping between partitions and
   * workers.
   */
  void fixPartitionIdToSocketAddrMap();

  /**
   * Sends a message to destination vertex.
   *
   * @param destVertexId Destination vertex id.
   * @param message Message to send.
   */
  void sendMessageReq(I destVertexId, M message);

  /**
   * Sends a partition to the appropriate partition owner
   *
   * @param workerInfo Owner the vertices belong to
   * @param partition Partition to send
   */
  void sendPartitionReq(WorkerInfo workerInfo,
      Partition<I, V, E, M> partition);

  /**
   * Sends a request to the appropriate vertex range owner to add an edge
   *
   * @param vertexIndex Index of the vertex to get the request
   * @param edge Edge to be added
   * @throws IOException
   */
  void addEdgeReq(I vertexIndex, Edge<I, E> edge) throws IOException;

  /**
   * Sends a request to the appropriate vertex range owner to remove an edge
   *
   * @param vertexIndex Index of the vertex to get the request
   * @param destinationVertexIndex Index of the edge to be removed
   * @throws IOException
   */
  void removeEdgeReq(I vertexIndex, I destinationVertexIndex)
    throws IOException;

  /**
   * Sends a request to the appropriate vertex range owner to add a vertex
   *
   * @param vertex Vertex to be added
   * @throws IOException
   */
  void addVertexReq(BasicVertex<I, V, E, M> vertex) throws IOException;

  /**
   * Sends a request to the appropriate vertex range owner to remove a vertex
   *
   * @param vertexIndex Index of the vertex to be removed
   * @throws IOException
   */
  void removeVertexReq(I vertexIndex) throws IOException;

  /**
   * Flush all outgoing messages.  This will synchronously ensure that all
   * messages have been send and delivered prior to returning.
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Get the messages sent during this superstep and clear them.
   *
   * @return Number of messages sent before the reset.
   */
  long resetMessageCount();

  /**
   * Closes all connections.
   *
   * @throws IOException
   */
  void closeConnections() throws IOException;
}
