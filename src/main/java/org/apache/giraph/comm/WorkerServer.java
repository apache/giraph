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

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for message communication server.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public interface WorkerServer<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends Closeable {
  /**
   * Get the port
   *
   * @return Port used by this server
   */
  int getPort();

  /**
   * Move the in transition messages to the in messages for every vertex and
   * add new connections to any newly appearing RPC proxies.
   */
  void prepareSuperstep();

  /**
   * Get the vertices that were sent in the last iteration.  After getting
   * the map, the user should synchronize with it to insure it
   * is thread-safe.
   *
   * @return map of vertex ranges to vertices
   */
  Map<Integer, Collection<BasicVertex<I, V, E, M>>> getInPartitionVertexMap();

  /**
   * Shuts down.
   */
  void close();
}
