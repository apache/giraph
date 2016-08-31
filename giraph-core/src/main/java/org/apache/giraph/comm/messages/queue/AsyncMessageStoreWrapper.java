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
package org.apache.giraph.comm.messages.queue;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * This class decouples message receiving and processing
 * into separate threads thus reducing contention.
 * It does not provide message store functionality itself, rather
 * providing a wrapper around existing message stores that
 * can now be used in async mode with only slight modifications.
 * @param <I> Vertex id
 * @param <M> Message data
 */
public final class AsyncMessageStoreWrapper<I extends WritableComparable,
    M extends Writable> implements MessageStore<I, M> {

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(AsyncMessageStoreWrapper.class);
  /** Pass this id to clear the queues and shutdown all threads
   * started by this processor */
  private static final PartitionMessage SHUTDOWN_QUEUE_MESSAGE =
      new PartitionMessage(-1, null);
  /** Pass this message to clear the queues but keep threads alive */
  private static final PartitionMessage CLEAR_QUEUE_MESSAGE =
      new PartitionMessage(-1, null);
  /** Executor that processes messages in background */
  private static final ExecutorService EXECUTOR_SERVICE =
      Executors.newCachedThreadPool(
          ThreadUtils.createThreadFactory("AsyncMessageStoreWrapper-%d"));

  /** Number of threads that will process messages in background */
  private final int threadsCount;
  /** Queue that temporary stores messages */
  private final BlockingQueue<PartitionMessage<I, M>>[] queues;
  /** Map from partition id to thread that process this partition */
  private final Int2IntMap partition2Queue;
  /** Signals that all procesing is done */
  private Semaphore completionSemaphore;
  /** Underlying message store */
  private final MessageStore<I, M> store;

  /**
   * Constructs async wrapper around existing message store
   * object. Requires partition list and number of threads
   * to properly initialize background threads and assign partitions.
   * Partitions are assigned to threads in round-robin fashion.
   * It guarantees that all threads have almost the same number of
   * partitions (+-1) no matter how partitions are assigned to this worker.
   * @param store underlying message store to be used in computation
   * @param partitions partitions assigned to this worker
   * @param threadCount number of threads that will be used to process
   *                    messages.
   */
  public AsyncMessageStoreWrapper(MessageStore<I, M> store,
                                  Iterable<Integer> partitions,
                                  int threadCount) {
    this.store = store;
    this.threadsCount = threadCount;
    completionSemaphore = new Semaphore(1 - threadsCount);
    queues = new BlockingQueue[threadsCount];
    partition2Queue = new Int2IntArrayMap();
    LOG.info("AsyncMessageStoreWrapper enabled. Threads= " + threadsCount);

    for (int i = 0; i < threadsCount; i++) {
      queues[i] = new LinkedBlockingQueue<>();
      EXECUTOR_SERVICE.submit(new MessageStoreQueueWorker(queues[i]));
    }

    int cnt = 0;
    for (int partitionId : partitions) {
      partition2Queue.put(partitionId, cnt++ % threadsCount);
    }

  }

  @Override
  public boolean isPointerListEncoding() {
    return store.isPointerListEncoding();
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    return store.getVertexMessages(vertexId);
  }

  @Override
  public void clearVertexMessages(I vertexId) {
    store.clearVertexMessages(vertexId);
  }

  @Override
  public void clearAll() {
    try {
      for (BlockingQueue<PartitionMessage<I, M>> queue : queues) {
        queue.put(SHUTDOWN_QUEUE_MESSAGE);
      }
      completionSemaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    store.clearAll();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return store.hasMessagesForVertex(vertexId);
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    return store.hasMessagesForPartition(partitionId);
  }

  @Override
  public void addPartitionMessages(
      int partitionId, VertexIdMessages<I, M> messages) {
    int hash = partition2Queue.get(partitionId);
    try {
      queues[hash].put(new PartitionMessage<>(partitionId, messages));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addMessage(I vertexId, M message) throws IOException {
    // TODO: implement if LocalBlockRunner needs async message store
    throw new UnsupportedOperationException();
  }

  @Override
  public void finalizeStore() {
    store.finalizeStore();
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    return store.getPartitionDestinationVertices(partitionId);
  }

  @Override
  public void clearPartition(int partitionId) {
    store.clearPartition(partitionId);
  }

  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
    store.writePartition(out, partitionId);
  }

  @Override
  public void readFieldsForPartition(DataInput in, int partitionId)
    throws IOException {
    store.readFieldsForPartition(in, partitionId);
  }

  /**
   * Wait till all messages are processed and all queues are empty.
   */
  public void waitToComplete() {
    try {
      for (BlockingQueue<PartitionMessage<I, M>> queue : queues) {
        queue.put(CLEAR_QUEUE_MESSAGE);
      }
      completionSemaphore.acquire();
      completionSemaphore = new Semaphore(1 - threadsCount);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This runnable has logic for background thread
   * that actually does message processing.
   */
  private class MessageStoreQueueWorker implements Runnable {
    /**
     * Queue assigned to this background thread.
     */
    private final BlockingQueue<PartitionMessage<I, M>> queue;

    /**
     * Constructs runnable.
     * @param queue where messages are put by client
     */
    private MessageStoreQueueWorker(
        BlockingQueue<PartitionMessage<I, M>> queue) {
      this.queue = queue;
    }

    @Override
    public void run() {
      PartitionMessage<I, M> message = null;
      while (true) {
        try {
          message = queue.take();
          if (message.getMessage() != null) {
            int partitionId = message.getPartitionId();
            store.addPartitionMessages(partitionId, message.getMessage());
          } else {
            completionSemaphore.release();
            if (message == SHUTDOWN_QUEUE_MESSAGE) {
              return;
            }
          }
        } catch (InterruptedException e) {
          LOG.error("MessageStoreQueueWorker.run: " + message, e);
          return;
        }
      }
    }
  }
}
