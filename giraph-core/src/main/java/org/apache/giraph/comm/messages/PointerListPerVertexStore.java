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

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedByteArrayOutputBuffer;
import org.apache.giraph.utils.ExtendedByteArrayOutputBuffer.IndexAndDataOut;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Implementation of {@link SimpleMessageStore} where multiple messages are
 * stored as a list of long pointers to extended data output objects
 * Used when there is no combiner provided.
 *
 * @param <I> vertexId type
 * @param <M> message type
 */
public class PointerListPerVertexStore<I extends WritableComparable,
  M extends Writable> extends AbstractListPerVertexStore<I, M, LongArrayList> {

  /** Buffers of byte array outputs used to store messages - thread safe */
  private final ExtendedByteArrayOutputBuffer bytesBuffer;

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param partitionInfo Partition split info
   * @param config Hadoop configuration
   */
  public PointerListPerVertexStore(
    MessageValueFactory<M> messageValueFactory,
    PartitionSplitInfo<I> partitionInfo,
    ImmutableClassesGiraphConfiguration<I, ?, ?> config
  ) {
    super(messageValueFactory, partitionInfo, config);
    bytesBuffer = new ExtendedByteArrayOutputBuffer(config);
  }

  @Override
  public boolean isPointerListEncoding() {
    return true;
  }

  @Override
  protected LongArrayList createList() {
    return new LongArrayList();
  }

  @Override
  public void addPartitionMessages(
    int partitionId, VertexIdMessages<I, M> messages) {
    try {
      VertexIdMessageIterator<I, M> vertexIdMessageIterator =
          messages.getVertexIdMessageIterator();
      long pointer = 0;
      LongArrayList list;
      while (vertexIdMessageIterator.hasNext()) {
        vertexIdMessageIterator.next();
        M msg = vertexIdMessageIterator.getCurrentMessage();
        list = getOrCreateList(vertexIdMessageIterator);
        if (vertexIdMessageIterator.isNewMessage()) {
          IndexAndDataOut indexAndDataOut = bytesBuffer.getIndexAndDataOut();
          pointer = indexAndDataOut.getIndex();
          pointer <<= 32;
          ExtendedDataOutput dataOutput = indexAndDataOut.getDataOutput();
          pointer += dataOutput.getPos();
          msg.write(dataOutput);
        }
        synchronized (list) {
          list.add(pointer);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("addPartitionMessages: IOException while" +
          " adding messages for a partition: " + e);
    }
  }

  @Override
  public void addMessage(I vertexId, M message) throws IOException {
    LongArrayList list = getOrCreateList(vertexId);
    IndexAndDataOut indexAndDataOut = bytesBuffer.getIndexAndDataOut();
    long pointer = indexAndDataOut.getIndex();
    pointer <<= 32;
    ExtendedDataOutput dataOutput = indexAndDataOut.getDataOutput();
    pointer += dataOutput.getPos();
    message.write(dataOutput);

    synchronized (list) {
      list.add(pointer);
    }
  }

  /**
   * Get messages as an iterable from message storage
   *
   * @param pointers list of pointers to messages
   * @return Messages as an iterable
   */
  @Override
  public Iterable<M> getMessagesAsIterable(LongArrayList pointers) {
    return new PointerListMessagesIterable<>(messageValueFactory, pointers,
      bytesBuffer);
  }

  @Override
  protected int getNumberOfMessagesIn(ConcurrentMap<I,
    LongArrayList> partitionMap) {
    int numberOfMessages = 0;
    for (LongArrayList list : partitionMap.values()) {
      numberOfMessages += list.size();
    }
    return numberOfMessages;
  }

  // FIXME -- complete these for check-pointing
  @Override
  protected void writeMessages(LongArrayList messages, DataOutput out)
    throws IOException {

  }

  @Override
  protected LongArrayList readFieldsForMessages(DataInput in)
    throws IOException {
    return null;
  }
}
