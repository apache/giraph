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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Factory for message stores
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <MS> Message store
 */
public interface MessageStoreFactory<I extends WritableComparable,
    M extends Writable, MS> {
  /**
   * Creates new message store.
   *
   * Note: MessageCombiner class in Configuration can be changed,
   * this method should return MessageStore which uses current combiner
   *
   *
   * @param messageValueFactory Message class held in the store
   * @return New message store
   */
  MS newStore(MessageValueFactory<M> messageValueFactory);

  /**
   * Implementation class should use this method of initialization
   * of any required internal state.
   *
   * @param service Service to get partition mappings
   * @param conf Configuration
   */
  void initialize(CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf);

  /**
   * This method is more for the performance optimization. If the message
   * traversal would be done in order then data structure which is optimized
   * for such traversal can be used.
   *
   * @return true if the messages would be traversed in order
   * else return false
   */
  boolean shouldTraverseMessagesInOrder();
}
