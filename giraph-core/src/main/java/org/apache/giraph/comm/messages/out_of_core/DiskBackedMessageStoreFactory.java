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

package org.apache.giraph.comm.messages.out_of_core;

import static org.apache.giraph.conf.GiraphConstants.MAX_MESSAGES_IN_MEMORY;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message store factory which persist the messages on the disk.
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
public class DiskBackedMessageStoreFactory<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, M>> {
  /** Service worker */
  private CentralizedServiceWorker<I, V, E> service;
  /** Number of messages to keep in memory */
  private int maxMessagesInMemory;
  /** Factory for creating file stores when flushing */
  private MessageStoreFactory<I, M,
    PartitionDiskBackedMessageStore<I, M>> fileStoreFactory;

  /**
   * Default constructor class helps in class invocation via Reflection
   */
  public DiskBackedMessageStoreFactory() {
  }

  /**
   * @param service Service worker
   * @param maxMessagesInMemory Number of messages to keep in memory
   * @param fileStoreFactory Factory for creating file stores when flushing
   */
  public DiskBackedMessageStoreFactory(
      CentralizedServiceWorker<I, V, E> service,
      int maxMessagesInMemory,
      MessageStoreFactory<I, M,
        PartitionDiskBackedMessageStore<I, M>> fileStoreFactory) {
    this.service = service;
    this.maxMessagesInMemory = maxMessagesInMemory;
    this.fileStoreFactory = fileStoreFactory;
  }

  @Override
  public MessageStore<I, M>
  newStore(MessageValueFactory<M> messageValueFactory) {
    return new DiskBackedMessageStore<I, V, E, M>(messageValueFactory,
        service, maxMessagesInMemory, fileStoreFactory);
  }

  @Override
  public void initialize(CentralizedServiceWorker service,
      ImmutableClassesGiraphConfiguration conf) {
    this.maxMessagesInMemory = MAX_MESSAGES_IN_MEMORY.get(conf);

    MessageStoreFactory<I, Writable, SequentialFileMessageStore<I, Writable>>
      fileMessageStoreFactory =
        SequentialFileMessageStore.newFactory(conf);
    this.fileStoreFactory =
        PartitionDiskBackedMessageStore.newFactory(conf,
            fileMessageStoreFactory);

    this.service = service;
  }

  @Override
  public boolean shouldTraverseMessagesInOrder() {
    return true;
  }
}
