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

import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Structure that keeps message information.
 *
 * @param <I> Vertex id
 */
public interface MessageData<I extends WritableComparable> {
  /**
   * Get message store for incoming messages (messages which will be consumed
   * in the next super step)
   *
   * @param <M> Message data type
   * @return Incoming message store
   */
  <M extends Writable> MessageStore<I, M> getIncomingMessageStore();

  /**
   * Get message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   *
   * @param <M> Message data type
   * @return Current message store
   */
  <M extends Writable> MessageStore<I, M> getCurrentMessageStore();

  /**
   * Re-initialize message stores.
   * Discards old values if any.

   * @throws IOException
   */
  void resetMessageStores() throws IOException;

  /**
   * Adds messages for partition to current message store
   *
   * @param <M> Message data type
   * @param partitionId Id of partition
   * @param messages    Collection of vertex ids and messages we want to add
   * @throws IOException
   */
  <M extends Writable> void addPartitionCurrentMessages(
      int partitionId, VertexIdMessages<I, M> messages)
      throws IOException;

  /**
   * Adds messages for partition to incoming message store
   *
   * @param <M> Message data type
   * @param partitionId Id of partition
   * @param messages    Collection of vertex ids and messages we want to add
   * @throws IOException
   */
  <M extends Writable> void addPartitionIncomingMessages(
      int partitionId, VertexIdMessages<I, M> messages)
      throws IOException;
}
