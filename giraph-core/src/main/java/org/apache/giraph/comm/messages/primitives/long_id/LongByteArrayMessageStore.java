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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.VertexIdMessageBytesIterator;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> Message type
 */
public class LongByteArrayMessageStore<M extends Writable>
  extends LongAbstractMessageStore<M, DataInputOutput> {

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param service             Service worker
   * @param config              Hadoop configuration
   */
  public LongByteArrayMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<LongWritable, Writable, Writable> service,
      ImmutableClassesGiraphConfiguration<LongWritable,
          Writable, Writable> config) {
    super(messageValueFactory, service, config);
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  /**
   * Get the DataInputOutput for a vertex id, creating if necessary.
   *
   * @param partitionMap Partition map to look in
   * @param vertexId Id of the vertex
   * @return DataInputOutput for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap, long vertexId) {
    DataInputOutput dataInputOutput = partitionMap.get(vertexId);
    if (dataInputOutput == null) {
      dataInputOutput = config.createMessagesInputOutput();
      partitionMap.put(vertexId, dataInputOutput);
    }
    return dataInputOutput;
  }

  @Override
  public void addPartitionMessages(int partitionId,
    VertexIdMessages<LongWritable, M> messages) throws IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageBytesIterator<LongWritable, M>
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
        VertexIdMessageIterator<LongWritable, M>
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
  public Iterable<M> getVertexMessages(
    LongWritable vertexId) throws IOException {
    DataInputOutput dataInputOutput =
        getPartitionMap(vertexId).get(vertexId.get());
    if (dataInputOutput == null) {
      return EmptyIterable.get();
    } else {
      return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
    }
  }

  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    out.writeInt(partitionMap.size());
    ObjectIterator<Long2ObjectMap.Entry<DataInputOutput>> iterator =
        partitionMap.long2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2ObjectMap.Entry<DataInputOutput> entry = iterator.next();
      out.writeLong(entry.getLongKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
    int partitionId) throws IOException {
    int size = in.readInt();
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        new Long2ObjectOpenHashMap<DataInputOutput>(size);
    while (size-- > 0) {
      long vertexId = in.readLong();
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      partitionMap.put(vertexId, dataInputOutput);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
