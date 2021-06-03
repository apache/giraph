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
package org.apache.giraph.comm.messages.primitives;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.VertexIdMessageBytesIterator;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Special message store to be used when IDs are primitive and no combiner is
 * used.
 * Data is backed by primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
public class IdByteArrayMessageStore<I extends WritableComparable,
    M extends Writable> implements MessageStore<I, M> {
  /** Message value factory */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Basic2ObjectMap<I, DataInputOutput>> map;
  /** Partition split info */
  private final PartitionSplitInfo<I> partitionInfo;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;
  /** Vertex id TypeOps */
  private final PrimitiveIdTypeOps<I> idTypeOps;
  /** WritableWriter for values in this message store */
  private final WritableWriter<DataInputOutput>
  dataInputOutputWriter = new WritableWriter<DataInputOutput>() {
    @Override
    public DataInputOutput readFields(DataInput in) throws IOException {
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      return dataInputOutput;
    }

    @Override
    public void write(DataOutput out, DataInputOutput value)
      throws IOException {
      value.write(out);
    }
  };

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param partitionInfo Partition split info
   * @param config Hadoop configuration
   */
  public IdByteArrayMessageStore(MessageValueFactory<M> messageValueFactory,
    PartitionSplitInfo<I> partitionInfo,
    ImmutableClassesGiraphConfiguration<I, ?, ?> config
  ) {
    this.messageValueFactory = messageValueFactory;
    this.partitionInfo = partitionInfo;
    this.config = config;

    idTypeOps = TypeOpsUtils.getPrimitiveIdTypeOps(config.getVertexIdClass());

    map = new Int2ObjectOpenHashMap<Basic2ObjectMap<I, DataInputOutput>>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      int capacity = Math.max(10,
        (int) partitionInfo.getPartitionVertexCount(partitionId));
      Basic2ObjectMap<I, DataInputOutput> partitionMap =
        idTypeOps.create2ObjectOpenHashMap(
          capacity,
          dataInputOutputWriter);

      map.put(partitionId, partitionMap);
    }
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  private Basic2ObjectMap<I, DataInputOutput> getPartitionMap(I vertexId) {
    return map.get(partitionInfo.getPartitionId(vertexId));
  }

  /**
   * Get the DataInputOutput for a vertex id, creating if necessary.
   *
   * @param partitionMap Partition map to look in
   * @param vertexId Id of the vertex
   * @return DataInputOutput for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      Basic2ObjectMap<I, DataInputOutput> partitionMap,
      I vertexId) {
    DataInputOutput dataInputOutput = partitionMap.get(vertexId);
    if (dataInputOutput == null) {
      dataInputOutput = config.createMessagesInputOutput();
      partitionMap.put(vertexId, dataInputOutput);
    }
    return dataInputOutput;
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<I, M> messages) {
    Basic2ObjectMap<I, DataInputOutput> partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageBytesIterator<I, M> vertexIdMessageBytesIterator =
          messages.getVertexIdMessageBytesIterator();
      // Try to copy the message buffer over rather than
      // doing a deserialization of a message just to know its size. This
      // should be more efficient for complex objects where serialization is
      // expensive. If this type of iterator is not available, fall back to
      // deserializing/serializing the messages
      if (vertexIdMessageBytesIterator != null) {
        while (vertexIdMessageBytesIterator.hasNext()) {
          vertexIdMessageBytesIterator.next();
          DataInputOutput dataInputOutput = getDataInputOutput(
              partitionMap, vertexIdMessageBytesIterator.getCurrentVertexId());
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
              dataInputOutput.getDataOutput());
        }
      } else {
        try {
          VertexIdMessageIterator<I, M> iterator =
              messages.getVertexIdMessageIterator();
          while (iterator.hasNext()) {
            iterator.next();
            DataInputOutput dataInputOutput =
                getDataInputOutput(partitionMap, iterator.getCurrentVertexId());

            VerboseByteStructMessageWrite.verboseWriteCurrentMessage(iterator,
                dataInputOutput.getDataOutput());
          }
        } catch (IOException e) {
          throw new RuntimeException("addPartitionMessages: IOException while" +
              " adding message for a partition: " + e);
        }
      }
    }
  }

  /**
   * Adds a message for a particular vertex
   *
   * @param vertexId Id of target vertex
   * @param message  A message to send
   * @throws IOException
   */
  @Override
  public void addMessage(I vertexId, M message) throws IOException {
    Basic2ObjectMap<I, DataInputOutput> partitionMap =
      getPartitionMap(vertexId);
    synchronized (partitionMap) {
      DataInputOutput dataInputOutput = getDataInputOutput(
        partitionMap, vertexId);
      VerboseByteStructMessageWrite.verboseWriteCurrentMessage(
        vertexId, message, dataInputOutput.getDataOutput());
    }
  }

  @Override
  public void clearPartition(int partitionId) {
    map.get(partitionId).clear();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId);
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Basic2ObjectMap<I, DataInputOutput> partitionMessages =
        map.get(partitionId);
    return partitionMessages != null && partitionMessages.size() != 0;
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    DataInputOutput dataInputOutput = getPartitionMap(vertexId).get(vertexId);
    if (dataInputOutput == null) {
      return EmptyIterable.get();
    } else {
      return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
    }
  }

  @Override
  public void clearVertexMessages(I vertexId) {
    getPartitionMap(vertexId).remove(vertexId);
  }

  @Override
  public void clearAll() {
    map.clear();
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    Basic2ObjectMap<I, DataInputOutput> partitionMap = map.get(partitionId);
    List<I> vertices = Lists.newArrayListWithCapacity(partitionMap.size());
    Iterator<I> iterator = partitionMap.fastKeyIterator();
    while (iterator.hasNext()) {
      vertices.add(idTypeOps.createCopy(iterator.next()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
    Basic2ObjectMap<I, DataInputOutput> partitionMap = map.get(partitionId);
    partitionMap.write(out);
  }

  @Override
  public void readFieldsForPartition(DataInput in, int partitionId)
    throws IOException {
    Basic2ObjectMap<I, DataInputOutput> partitionMap =
        idTypeOps.create2ObjectOpenHashMap(dataInputOutputWriter);
    partitionMap.readFields(in);
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }
}
