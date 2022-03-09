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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message store
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageStore<I extends WritableComparable,
    M extends Writable> {
  /**
   * True if this message-store encodes messages as a list of long pointers
   * to compact serialized messages
   *
   * @return true if we encode messages as a list of pointers
   */
  boolean isPointerListEncoding();

  /**
   * Gets messages for a vertex.  The lifetime of every message is only
   * guaranteed until the iterator's next() method is called. Do not hold
   * references to objects returned by this iterator.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Iterable of messages for a vertex id
   */
  Iterable<M> getVertexMessages(I vertexId);

  /**
   * Clears messages for a vertex.
   *
   * @param vertexId Vertex id for which we want to clear messages
   */
  void clearVertexMessages(I vertexId);

  /**
   * Clears all resources used by this store.
   */
  void clearAll();

  /**
   * Check if we have messages for some vertex
   *
   * @param vertexId Id of vertex which we want to check
   * @return True iff we have messages for vertex with required id
   */
  boolean hasMessagesForVertex(I vertexId);

  /**
   * Check if we have messages for some partition
   *
   * @param partitionId Id of partition which we want to check
   * @return True iff we have messages for the given partition
   */
  boolean hasMessagesForPartition(int partitionId);

  /**
   * Adds messages for partition
   *
   * @param partitionId Id of partition
   * @param messages    Collection of vertex ids and messages we want to add
   */
  void addPartitionMessages(
      int partitionId, VertexIdMessages<I, M> messages);

  /**
   * Adds a message for a particular vertex
   * The method is used by InternalMessageStore to send local messages; for
   * the general case, use a more efficient addPartitionMessages
   *
   * @param vertexId Id of target vertex
   * @param message  A message to send
   * @throws IOException
   */
  void addMessage(I vertexId, M message) throws IOException;

  /**
   * Called before start of computation in bspworker
   * Since it is run from a single thread while the store is not being
   * accessed by any other thread - this is ensured to be thread-safe
   */
  void finalizeStore();

  /**
   * Gets vertex ids from selected partition which we have messages for
   *
   * @param partitionId Id of partition
   * @return Iterable over vertex ids which we have messages for
   */
  Iterable<I> getPartitionDestinationVertices(int partitionId);

  /**
   * Clears messages for a partition.
   *
   * @param partitionId Partition id for which we want to clear messages
   */
  void clearPartition(int partitionId);

  /**
   * Serialize messages for one partition.
   *
   * @param out         {@link DataOutput} to serialize this object into
   * @param partitionId Id of partition
   * @throws IOException
   */
  void writePartition(DataOutput out, int partitionId) throws IOException;

  /**
   * Deserialize messages for one partition
   *
   * @param in          {@link DataInput} to deserialize this object
   *                    from.
   * @param partitionId Id of partition
   * @throws IOException
   */
  void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException;
}
