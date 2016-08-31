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

package org.apache.giraph.comm.messages.primitives.long_id;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.comm.messages.PointerListMessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.ExtendedByteArrayOutputBuffer;
import org.apache.giraph.utils.ExtendedByteArrayOutputBuffer.IndexAndDataOut;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * This stores messages in
 * {@link org.apache.giraph.utils.ExtendedByteArrayOutputBuffer}
 * and stores long pointers that point to serialized messages
 *
 * @param <M> message type
 */
public class LongPointerListPerVertexStore<M extends Writable>
  extends LongAbstractListStore<M, LongArrayList>
  implements MessageStore<LongWritable, M> {

  /** Buffers of byte array outputs used to store messages - thread safe */
  private final ExtendedByteArrayOutputBuffer bytesBuffer;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param partitionInfo       Partition split info
   * @param config              Hadoop configuration
   */
  public LongPointerListPerVertexStore(
    MessageValueFactory<M> messageValueFactory,
    PartitionSplitInfo<LongWritable> partitionInfo,
    ImmutableClassesGiraphConfiguration<LongWritable,
    Writable, Writable> config) {
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
    int partitionId,
    VertexIdMessages<LongWritable, M> messages
  ) {
    try {
      VertexIdMessageIterator<LongWritable, M> iterator =
          messages.getVertexIdMessageIterator();
      long pointer = 0;
      LongArrayList list;
      while (iterator.hasNext()) {
        iterator.next();
        M msg = iterator.getCurrentMessage();
        list = getList(iterator.getCurrentVertexId());

        if (iterator.isNewMessage()) {
          IndexAndDataOut indexAndDataOut = bytesBuffer.getIndexAndDataOut();
          pointer = indexAndDataOut.getIndex();
          pointer <<= 32;
          ExtendedDataOutput dataOutput = indexAndDataOut.getDataOutput();
          pointer += dataOutput.getPos();
          msg.write(dataOutput);
        }
        synchronized (list) { // TODO - any better way?
          list.add(pointer);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("addPartitionMessages: IOException while" +
          " adding messages for a partition: " + e);
    }
  }

  @Override
  public void addMessage(LongWritable vertexId, M message) throws IOException {
    LongArrayList list = getList(vertexId);
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

  @Override
  public Iterable<M> getVertexMessages(LongWritable vertexId) {
    LongArrayList list = getPartitionMap(vertexId).get(
        vertexId.get());
    if (list == null) {
      return EmptyIterable.get();
    } else {
      return new PointerListMessagesIterable<>(messageValueFactory,
        list, bytesBuffer);
    }
  }

  // FIXME -- complete these for check-pointing
  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
  }

  @Override
  public void readFieldsForPartition(DataInput in, int partitionId)
    throws IOException {
  }
}
