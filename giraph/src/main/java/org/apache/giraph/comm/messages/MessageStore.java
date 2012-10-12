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

/**
 * Message store
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageStore<I extends WritableComparable,
    M extends Writable> extends BasicMessageStore<I, M> {
  /**
   * Adds messages
   *
   * @param vertexId Vertex id for which the messages are
   * @param messages Messages for the vertex
   * @throws IOException
   */
  void addVertexMessages(I vertexId,
      Collection<M> messages) throws IOException;

  /**
   * Get number of messages in memory
   *
   * @return Number of messages in memory
   */
  int getNumberOfMessages();

  /**
   * Check if we have messages for some vertex
   *
   * @param vertexId Id of vertex which we want to check
   * @return True iff we have messages for vertex with required id
   */
  boolean hasMessagesForVertex(I vertexId);

  /**
   * Gets vertex ids which we have messages for
   *
   * @return Iterable over vertex ids which we have messages for
   */
  Iterable<I> getDestinationVertices();
}
