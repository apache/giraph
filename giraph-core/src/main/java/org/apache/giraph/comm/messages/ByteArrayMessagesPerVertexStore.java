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

import com.google.common.collect.Iterators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.RepresentativeByteStructIterator;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.VertexIdIterator;
import org.apache.giraph.utils.VertexIdMessageBytesIterator;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Implementation of {@link SimpleMessageStore} where multiple messages are
 * stored per vertex as byte backed datastructures.
 * Used when there is no combiner provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayMessagesPerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, DataInputOutput> {
  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param partitionInfo Partition split info
   * @param config Hadoop configuration
   */
  public ByteArrayMessagesPerVertexStore(
      MessageValueFactory<M> messageValueFactory,
      PartitionSplitInfo<I> partitionInfo,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, partitionInfo, config);
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  /**
   * Get the extended data output for a vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param partitionMap Partition map to look in
   * @param iterator Special iterator that can release ownerhips of vertex ids
   * @return Extended data output for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      ConcurrentMap<I, DataInputOutput> partitionMap,
      VertexIdIterator<I> iterator) {
    DataInputOutput dataInputOutput =
        partitionMap.get(iterator.getCurrentVertexId());
    if (dataInputOutput == null) {
      DataInputOutput newDataOutput = config.createMessagesInputOutput();
      dataInputOutput = partitionMap.putIfAbsent(
          iterator.releaseCurrentVertexId(), newDataOutput);
      if (dataInputOutput == null) {
        dataInputOutput = newDataOutput;
      }
    }
    return dataInputOutput;
  }

  @Override
  public void addPartitionMessages(
    int partitionId, VertexIdMessages<I, M> messages) {
    ConcurrentMap<I, DataInputOutput> partitionMap =
        getOrCreatePartitionMap(partitionId);
    VertexIdMessageBytesIterator<I, M> vertexIdMessageBytesIterator =
        messages.getVertexIdMessageBytesIterator();
    // Try to copy the message buffer over rather than
    // doing a deserialization of a message just to know its size.  This
    // should be more efficient for complex objects where serialization is
    // expensive.  If this type of iterator is not available, fall back to
    // deserializing/serializing the messages
    if (vertexIdMessageBytesIterator != null) {
      while (vertexIdMessageBytesIterator.hasNext()) {
        vertexIdMessageBytesIterator.next();
        DataInputOutput dataInputOutput =
            getDataInputOutput(partitionMap, vertexIdMessageBytesIterator);

        synchronized (dataInputOutput) {
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
              dataInputOutput.getDataOutput());
        }
      }
    } else {
      try {
        VertexIdMessageIterator<I, M> vertexIdMessageIterator =
            messages.getVertexIdMessageIterator();
        while (vertexIdMessageIterator.hasNext()) {
          vertexIdMessageIterator.next();
          DataInputOutput dataInputOutput =
              getDataInputOutput(partitionMap, vertexIdMessageIterator);

          synchronized (dataInputOutput) {
            VerboseByteStructMessageWrite.verboseWriteCurrentMessage(
                vertexIdMessageIterator, dataInputOutput.getDataOutput());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("addPartitionMessages: IOException while" +
            " adding messages for a partition: " + e);
      }
    }
  }

  @Override
  public void addMessage(I vertexId, M message) throws IOException {
    ConcurrentMap<I, DataInputOutput> partitionMap =
      getOrCreatePartitionMap(getPartitionId(vertexId));
    DataInputOutput dataInputOutput = partitionMap.get(vertexId);
    if (dataInputOutput == null) {
      DataInputOutput newDataOutput = config.createMessagesInputOutput();
      I copyId = WritableUtils.createCopy(vertexId);
      dataInputOutput = partitionMap.putIfAbsent(copyId, newDataOutput);
      if (dataInputOutput == null) {
        dataInputOutput = newDataOutput;
      }
    }

    synchronized (dataInputOutput) {
      VerboseByteStructMessageWrite.verboseWriteCurrentMessage(
        vertexId, message, dataInputOutput.getDataOutput());
    }
  }

  @Override
  protected Iterable<M> getMessagesAsIterable(
      DataInputOutput dataInputOutput) {
    return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
  }

  @Override
  protected int getNumberOfMessagesIn(
      ConcurrentMap<I, DataInputOutput> partitionMap) {
    int numberOfMessages = 0;
    for (DataInputOutput dataInputOutput : partitionMap.values()) {
      numberOfMessages += Iterators.size(
          new RepresentativeByteStructIterator<M>(
              dataInputOutput.createDataInput()) {
            @Override
            protected M createWritable() {
              return messageValueFactory.newInstance();
            }
          });
    }
    return numberOfMessages;
  }

  @Override
  protected void writeMessages(DataInputOutput dataInputOutput,
      DataOutput out) throws IOException {
    dataInputOutput.write(out);
  }

  @Override
  protected DataInputOutput readFieldsForMessages(DataInput in) throws
      IOException {
    DataInputOutput dataInputOutput = config.createMessagesInputOutput();
    dataInputOutput.readFields(in);
    return dataInputOutput;
  }

  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, MessageStore<I, M>> newFactory(
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    return new Factory<I, M>(service, config);
  }

  /**
   * Factory for {@link ByteArrayMessagesPerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  public static class Factory<I extends WritableComparable, M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, M>> {
    /** Service worker */
    private PartitionSplitInfo<I> partitionInfo;
    /** Hadoop configuration */
    private ImmutableClassesGiraphConfiguration<I, ?, ?> config;

    /** Constructor for reflection */
    public Factory() { }

    /**
     * @param partitionInfo Partition split info
     * @param config  Hadoop configuration
     */
    public Factory(
      PartitionSplitInfo<I> partitionInfo,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config
    ) {
      this.partitionInfo = partitionInfo;
      this.config = config;
    }

    @Override
    public MessageStore<I, M> newStore(
        MessageClasses<I, M> messageClasses) {
      return new ByteArrayMessagesPerVertexStore<I, M>(
          messageClasses.createMessageValueFactory(config),
          partitionInfo, config);
    }

    @Override
    public void initialize(PartitionSplitInfo<I> partitionInfo,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
      this.partitionInfo = partitionInfo;
      this.config = conf;
    }
  }

}
