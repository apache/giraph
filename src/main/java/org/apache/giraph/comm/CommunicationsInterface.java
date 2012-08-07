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

import org.apache.giraph.graph.Vertex;
/*if[HADOOP_NON_SECURE]
 else[HADOOP_NON_SECURE]*/
import org.apache.giraph.hadoop.BspTokenSelector;
import org.apache.hadoop.security.token.TokenInfo;
/*end[HADOOP_NON_SECURE]*/
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;


/**
 * Basic interface for communication between workers.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
/*if[HADOOP_NON_SECURE]
 else[HADOOP_NON_SECURE]*/
@TokenInfo(BspTokenSelector.class)
/*end[HADOOP_NON_SECURE]*/
public interface CommunicationsInterface<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends VersionedProtocol {
  /**
   * Interface Version History
   *
   * 0 - First Version
   */
  long VERSION_ID = 0L;

  /**
   * Adds incoming message.
   *
   * @param vertexIndex Destination vertex index.
   * @param message Message to store.
   * @throws IOException
   */
  void putMsg(I vertexIndex, M message) throws IOException;

  /**
   * Adds incoming message list.
   *
   * @param vertexIndex Vertex index where the message are added
   * @param msgList messages added
   * @throws IOException
   */
  void putMsgList(I vertexIndex, MsgList<M> msgList) throws IOException;

  /**
   * Adds a list of vertex ids and their respective message lists.
   *
   * @param vertexIdMessagesList messages to be added
   * @throws IOException
   */
  void putVertexIdMessagesList(
      VertexIdMessagesList<I, M> vertexIdMessagesList) throws IOException;

  /**
   * Adds vertex list (index, value, edges, etc.) to the appropriate worker.
   *
   * @param partitionId Partition id of the vertices to be added.
   * @param vertexList List of vertices to add
   */
  void putVertexList(int partitionId,
      VertexList<I, V, E, M> vertexList) throws IOException;

  /**
   * Add an edge to a remote vertex
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   * @param edgeValue Edge value
   * @throws IOException
   */
  void addEdge(I sourceVertexId, I targetVertexId,
               E edgeValue) throws IOException;

  /**
   * Remove an edge on a remote vertex
   *
   * @param vertexIndex Vertex index where the edge is added
   * @param destinationVertexIndex Edge vertex index to be removed
   * @throws IOException
   */
  void removeEdge(I vertexIndex, I destinationVertexIndex) throws IOException;

  /**
   * Add a remote vertex
   *
   * @param vertex Vertex that will be added
   * @throws IOException
   */
  void addVertex(Vertex<I, V, E, M> vertex) throws IOException;

  /**
   * Removed a remote vertex
   *
   * @param vertexIndex Vertex index representing vertex to be removed
   * @throws IOException
   */
  void removeVertex(I vertexIndex) throws IOException;

  /**
   * @return The name of this worker in the format "hostname:port".
   */
  String getName();
}
