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

package org.apache.giraph.ooc.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.persistence.DataIndex;
import org.apache.giraph.ooc.persistence.DataIndex.NumericIndexEntry;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.giraph.utils.ByteArrayOneMessageToManyIds;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;


/**
 * Implementation of a message store used for out-of-core mechanism.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class DiskBackedMessageStore<I extends WritableComparable,
    M extends Writable> extends DiskBackedDataStore<VertexIdMessages<I, M>>
    implements MessageStore<I, M> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedMessageStore.class);
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;
  /** In-memory message store */
  private final MessageStore<I, M> messageStore;
  /** Whether the message store uses message combiner or not */
  private final boolean useMessageCombiner;
  /** Which superstep this message store is used for */
  private final long superstep;
  /** Message value class */
  private final MessageValueFactory<M> messageValueFactory;

  /**
   * Type of VertexIdMessage class (container for serialized messages) received
   * for a particular message. If we write the received messages to disk before
   * adding them to message store, we need this type when we want to read the
   * messages back from disk (so that we know how to read the messages from
   * disk).
   */
  private enum SerializedMessageClass {
    /** ByteArrayVertexIdMessages */
    BYTE_ARRAY_VERTEX_ID_MESSAGES,
    /** ByteArrayOneMEssageToManyIds */
    BYTE_ARRAY_ONE_MESSAGE_TO_MANY_IDS
  }

  /**
   * Constructor
   *
   * @param config Configuration
   * @param oocEngine Out-of-core engine
   * @param messageStore In-memory message store for which out-of-core message
   *                     store would be wrapper
   * @param useMessageCombiner Whether message combiner is used for this message
   *                           store
   * @param superstep superstep number this messages store is used for
   */
  public DiskBackedMessageStore(ImmutableClassesGiraphConfiguration<I, ?, ?>
                                    config,
                                OutOfCoreEngine oocEngine,
                                MessageStore<I, M> messageStore,
                                boolean useMessageCombiner, long superstep) {
    super(config, oocEngine);
    this.config = config;
    this.messageStore = messageStore;
    this.useMessageCombiner = useMessageCombiner;
    this.superstep = superstep;
    this.messageValueFactory = config.createOutgoingMessageValueFactory();
  }

  @Override
  public boolean isPointerListEncoding() {
    return messageStore.isPointerListEncoding();
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    return messageStore.getVertexMessages(vertexId);
  }

  @Override
  public void clearVertexMessages(I vertexId) {
    messageStore.clearVertexMessages(vertexId);
  }

  @Override
  public void clearAll() {
    messageStore.clearAll();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return messageStore.hasMessagesForVertex(vertexId);
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    return messageStore.hasMessagesForPartition(partitionId);
  }

  @Override
  public void addPartitionMessages(
      int partitionId, VertexIdMessages<I, M> messages) {
    if (useMessageCombiner) {
      messageStore.addPartitionMessages(partitionId, messages);
    } else {
      addEntry(partitionId, messages);
    }
  }

  @Override
  public void addMessage(I vertexId, M message) throws IOException {
    if (useMessageCombiner) {
      messageStore.addMessage(vertexId, message);
    } else {
      // TODO: implement if LocalBlockRunner needs this message store
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Gets the path that should be used specifically for message data.
   *
   * @param basePath path prefix to build the actual path from
   * @param superstep superstep for which message data should be stored
   * @return path to files specific for message data
   */
  private static String getPath(String basePath, long superstep) {
    return basePath + "_messages-S" + superstep;
  }

  @Override
  public long loadPartitionData(int partitionId)
      throws IOException {
    if (!useMessageCombiner) {
      return loadPartitionDataProxy(partitionId,
          new DataIndex().addIndex(DataIndex.TypeIndexEntry.MESSAGE)
              .addIndex(NumericIndexEntry.createSuperstepEntry(superstep)));
    } else {
      return 0;
    }
  }

  @Override
  public long offloadPartitionData(int partitionId)
      throws IOException {
    if (!useMessageCombiner) {
      return offloadPartitionDataProxy(partitionId,
          new DataIndex().addIndex(DataIndex.TypeIndexEntry.MESSAGE)
              .addIndex(NumericIndexEntry.createSuperstepEntry(superstep)));
    } else {
      return 0;
    }
  }

  @Override
  public long offloadBuffers(int partitionId)
      throws IOException {
    if (!useMessageCombiner) {
      return offloadBuffersProxy(partitionId,
          new DataIndex().addIndex(DataIndex.TypeIndexEntry.MESSAGE)
              .addIndex(NumericIndexEntry.createSuperstepEntry(superstep)));
    } else {
      return 0;
    }
  }

  @Override
  public void finalizeStore() {
    messageStore.finalizeStore();
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    return messageStore.getPartitionDestinationVertices(partitionId);
  }

  @Override
  public void clearPartition(int partitionId) {
    messageStore.clearPartition(partitionId);
  }

  @Override
  public void writePartition(DataOutput out, int partitionId)
      throws IOException {
    messageStore.writePartition(out, partitionId);
  }

  @Override
  public void readFieldsForPartition(DataInput in, int partitionId)
      throws IOException {
    messageStore.readFieldsForPartition(in, partitionId);
  }

  @Override
  protected void writeEntry(VertexIdMessages<I, M> messages, DataOutput out)
      throws IOException {
    SerializedMessageClass messageClass;
    if (messages instanceof ByteArrayVertexIdMessages) {
      messageClass = SerializedMessageClass.BYTE_ARRAY_VERTEX_ID_MESSAGES;
    } else if (messages instanceof ByteArrayOneMessageToManyIds) {
      messageClass = SerializedMessageClass.BYTE_ARRAY_ONE_MESSAGE_TO_MANY_IDS;
    } else {
      throw new IllegalStateException("writeEntry: serialized message " +
          "type is not supported");
    }
    out.writeByte(messageClass.ordinal());
    messages.write(out);
  }

  @Override
  protected VertexIdMessages<I, M> readNextEntry(DataInput in)
      throws IOException {
    byte messageType = in.readByte();
    SerializedMessageClass messageClass =
        SerializedMessageClass.values()[messageType];
    VertexIdMessages<I, M> vim;
    switch (messageClass) {
    case BYTE_ARRAY_VERTEX_ID_MESSAGES:
      vim = new ByteArrayVertexIdMessages<>(messageValueFactory);
      vim.setConf(config);
      break;
    case BYTE_ARRAY_ONE_MESSAGE_TO_MANY_IDS:
      vim = new ByteArrayOneMessageToManyIds<>(messageValueFactory);
      vim.setConf(config);
      break;
    default:
      throw new IllegalStateException("readNextEntry: unsupported " +
          "serialized message type!");
    }
    vim.readFields(in);
    return vim;
  }

  @Override
  protected long loadInMemoryPartitionData(int partitionId, int ioThreadId,
                                           DataIndex index) throws IOException {
    long numBytes = 0;
    if (hasPartitionDataOnFile.remove(partitionId)) {
      OutOfCoreDataAccessor.DataInputWrapper inputWrapper =
          oocEngine.getDataAccessor().prepareInput(ioThreadId, index.copy());
      messageStore.readFieldsForPartition(inputWrapper.getDataInput(),
          partitionId);
      numBytes = inputWrapper.finalizeInput(true);
    }
    return numBytes;
  }

  @Override
  protected long offloadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException {
    long numBytes = 0;
    if (messageStore.hasMessagesForPartition(partitionId)) {
      OutOfCoreDataAccessor.DataOutputWrapper outputWrapper =
          oocEngine.getDataAccessor().prepareOutput(ioThreadId, index.copy(),
              false);
      messageStore.writePartition(outputWrapper.getDataOutput(), partitionId);
      messageStore.clearPartition(partitionId);
      numBytes = outputWrapper.finalizeOutput();
      hasPartitionDataOnFile.add(partitionId);
    }
    return numBytes;
  }

  @Override
  protected int entrySerializedSize(VertexIdMessages<I, M> messages) {
    return messages.getSerializedSize();
  }

  @Override
  protected void addEntryToInMemoryPartitionData(int partitionId,
                                                 VertexIdMessages<I, M>
                                                     messages) {
    messageStore.addPartitionMessages(partitionId, messages);
  }
}
