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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.VertexIdMessageBytesIterator;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Special message store to be used when ids are IntWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> Message type
 */
public class IntByteArrayMessageStore<M extends Writable>
    implements MessageStore<IntWritable, M> {
  /** Message value factory */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Map from partition id to map from vertex id to message */
  private final
  Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<DataInputOutput>> map;
  /** Service worker */
  private final CentralizedServiceWorker<IntWritable, ?, ?> service;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<IntWritable, ?, ?> config;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param service      Service worker
   * @param config       Hadoop configuration
   */
  public IntByteArrayMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<IntWritable, Writable, Writable> service,
      ImmutableClassesGiraphConfiguration<IntWritable, Writable, Writable>
        config) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.config = config;

    map =
        new Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<DataInputOutput>>();
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Partition<IntWritable, Writable, Writable> partition =
          service.getPartitionStore().getOrCreatePartition(partitionId);
      Int2ObjectOpenHashMap<DataInputOutput> partitionMap =
          new Int2ObjectOpenHashMap<DataInputOutput>(
              (int) partition.getVertexCount());
      map.put(partitionId, partitionMap);
      service.getPartitionStore().putPartition(partition);
    }
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  private Int2ObjectOpenHashMap<DataInputOutput> getPartitionMap(
      IntWritable vertexId) {
    return map.get(service.getPartitionId(vertexId));
  }

  /**
   * Get the DataInputOutput for a vertex id, creating if necessary.
   *
   * @param partitionMap Partition map to look in
   * @param vertexId     Id of the vertex
   * @return DataInputOutput for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      Int2ObjectOpenHashMap<DataInputOutput> partitionMap,
      int vertexId) {
    DataInputOutput dataInputOutput = partitionMap.get(vertexId);
    if (dataInputOutput == null) {
      dataInputOutput = config.createMessagesInputOutput();
      partitionMap.put(vertexId, dataInputOutput);
    }
    return dataInputOutput;
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<IntWritable, M> messages) throws
      IOException {
    Int2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageBytesIterator<IntWritable, M>
          vertexIdMessageBytesIterator =
          messages.getVertexIdMessageBytesIterator();
      // Try to copy the message buffer over rather than
      // doing a deserialization of a message just to know its size.  This
      // should be more efficient for complex objects where serialization is
      // expensive.  If this type of iterator is not available, fall back to
      // deserializing/serializing the messages
      if (vertexIdMessageBytesIterator != null) {
        while (vertexIdMessageBytesIterator.hasNext()) {
          vertexIdMessageBytesIterator.next();
          DataInputOutput dataInputOutput = getDataInputOutput(partitionMap,
              vertexIdMessageBytesIterator.getCurrentVertexId().get());
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
              dataInputOutput.getDataOutput());
        }
      } else {
        VertexIdMessageIterator<IntWritable, M>
            iterator = messages.getVertexIdMessageIterator();
        while (iterator.hasNext()) {
          iterator.next();
          DataInputOutput dataInputOutput =  getDataInputOutput(partitionMap,
              iterator.getCurrentVertexId().get());
          VerboseByteStructMessageWrite.verboseWriteCurrentMessage(iterator,
              dataInputOutput.getDataOutput());
        }
      }
    }
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.get(partitionId).clear();
  }

  @Override
  public boolean hasMessagesForVertex(IntWritable vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId.get());
  }

  @Override
  public Iterable<M> getVertexMessages(
      IntWritable vertexId) throws IOException {
    DataInputOutput dataInputOutput =
        getPartitionMap(vertexId).get(vertexId.get());
    if (dataInputOutput == null) {
      return EmptyIterable.get();
    } else {
      return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
    }
  }

  @Override
  public void clearVertexMessages(IntWritable vertexId) throws IOException {
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  @Override
  public Iterable<IntWritable> getPartitionDestinationVertices(
      int partitionId) {
    Int2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    List<IntWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    IntIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new IntWritable(iterator.nextInt()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    Int2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    out.writeInt(partitionMap.size());
    ObjectIterator<Int2ObjectMap.Entry<DataInputOutput>> iterator =
        partitionMap.int2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<DataInputOutput> entry = iterator.next();
      out.writeInt(entry.getIntKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Int2ObjectOpenHashMap<DataInputOutput> partitionMap =
        new Int2ObjectOpenHashMap<DataInputOutput>(size);
    while (size-- > 0) {
      int vertexId = in.readInt();
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      partitionMap.put(vertexId, dataInputOutput);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
