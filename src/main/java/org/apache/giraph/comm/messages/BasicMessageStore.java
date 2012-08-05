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

package org.apache.giraph.comm.messages;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Most basic message store with just add, get and clear operations
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface BasicMessageStore<I extends WritableComparable,
    M extends Writable> extends Writable {
  /**
   * Adds messages
   *
   * @param messages Map of messages we want to add
   * @throws IOException
   */
  void addMessages(Map<I, Collection<M>> messages) throws IOException;

  /**
   * Gets messages for a vertex.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Messages for vertex with required id
   * @throws IOException
   */
  Collection<M> getVertexMessages(I vertexId) throws IOException;

  /**
   * Clears messages for a vertex.
   *
   * @param vertexId Vertex id for which we want to clear messages
   * @throws IOException
   */
  void clearVertexMessages(I vertexId) throws IOException;

  /**
   * Clears all resources used by this store.
   *
   * @throws IOException
   */
  void clearAll() throws IOException;
}
