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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Anything that the server stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class ServerData<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /**
   * Map of partition ids to incoming vertices from other workers.
   * (Synchronized on values)
   */
  private final ConcurrentHashMap<Integer, Collection<BasicVertex<I, V, E, M>>>
  inPartitionVertexMap =
      new ConcurrentHashMap<Integer, Collection<BasicVertex<I, V, E, M>>>();
  /**
   * Map of inbound messages, mapping from vertex index to list of messages.
   * Transferred to inMessages at beginning of a superstep.  This
   * intermediary step exists so that the combiner will run not only at the
   * client, but also at the server. Also, allows the sending of large
   * message lists during the superstep computation. (Synchronized on values)
   */
  private final ConcurrentHashMap<I, Collection<M>> transientMessages =
      new ConcurrentHashMap<I, Collection<M>>();
  /**
   * Map of partition ids to incoming vertex mutations from other workers.
   * (Synchronized access to values)
   */
  private final ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  vertexMutations = new ConcurrentHashMap<I, VertexMutations<I, V, E, M>>();

  /**
   * Get the partition vertices (synchronize on the values)
   *
   * @return Partition vertices
   */
  public ConcurrentHashMap<Integer, Collection<BasicVertex<I, V, E, M>>>
  getPartitionVertexMap() {
    return inPartitionVertexMap;
  }

  /**
   * Get the vertex messages (synchronize on the values)
   *
   * @return Vertex messages
   */
  public ConcurrentHashMap<I, Collection<M>> getTransientMessages() {
    return transientMessages;
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  getVertexMutations() {
    return vertexMutations;
  }
}
