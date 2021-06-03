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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.Lists;

/**
 * Special message store to be used when ids are LongWritable and messages
 * are DoubleWritable and messageCombiner is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 */
public class LongDoubleMessageStore
    implements MessageStore<LongWritable, DoubleWritable> {
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Long2DoubleOpenHashMap> map;
  /** Message messageCombiner */
  private final
  MessageCombiner<? super LongWritable, DoubleWritable> messageCombiner;
  /** Service worker */
  private final PartitionSplitInfo<LongWritable> partitionInfo;

  /**
   * Constructor
   *
   * @param partitionInfo Partition split info
   * @param messageCombiner Message messageCombiner
   */
  public LongDoubleMessageStore(
    PartitionSplitInfo<LongWritable> partitionInfo,
    MessageCombiner<? super LongWritable, DoubleWritable> messageCombiner
  ) {
    this.partitionInfo = partitionInfo;
    this.messageCombiner = messageCombiner;

    map = new Int2ObjectOpenHashMap<Long2DoubleOpenHashMap>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      Long2DoubleOpenHashMap partitionMap = new Long2DoubleOpenHashMap(
          (int) partitionInfo.getPartitionVertexCount(partitionId));
      map.put(partitionId, partitionMap);
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
  private Long2DoubleOpenHashMap getPartitionMap(LongWritable vertexId) {
    return map.get(partitionInfo.getPartitionId(vertexId));
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<LongWritable, DoubleWritable> messages) {
    LongWritable reusableVertexId = new LongWritable();
    DoubleWritable reusableMessage = new DoubleWritable();
    DoubleWritable reusableCurrentMessage = new DoubleWritable();

    Long2DoubleOpenHashMap partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageIterator<LongWritable, DoubleWritable> iterator =
        messages.getVertexIdMessageIterator();
      while (iterator.hasNext()) {
        iterator.next();
        long vertexId = iterator.getCurrentVertexId().get();
        double message = iterator.getCurrentMessage().get();
        if (partitionMap.containsKey(vertexId)) {
          reusableVertexId.set(vertexId);
          reusableMessage.set(message);
          reusableCurrentMessage.set(partitionMap.get(vertexId));
          messageCombiner.combine(reusableVertexId, reusableCurrentMessage,
              reusableMessage);
          message = reusableCurrentMessage.get();
        }
        // FIXME: messageCombiner should create an initial message instead
        partitionMap.put(vertexId, message);
      }
    }
  }

  @Override
  public void addMessage(
    LongWritable vertexId,
    DoubleWritable message
  ) throws IOException {
    Long2DoubleOpenHashMap partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      double originalValue = partitionMap.get(vertexId.get());
      DoubleWritable originalMessage = new DoubleWritable(originalValue);
      messageCombiner.combine(vertexId, originalMessage, message);
      partitionMap.put(vertexId.get(), originalMessage.get());
    }
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public void clearPartition(int partitionId) {
    map.get(partitionId).clear();
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId.get());
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Long2DoubleOpenHashMap partitionMessages = map.get(partitionId);
    return partitionMessages != null && !partitionMessages.isEmpty();
  }

  @Override
  public Iterable<DoubleWritable> getVertexMessages(
      LongWritable vertexId) {
    Long2DoubleOpenHashMap partitionMap = getPartitionMap(vertexId);
    if (!partitionMap.containsKey(vertexId.get())) {
      return EmptyIterable.get();
    } else {
      return Collections.singleton(
          new DoubleWritable(partitionMap.get(vertexId.get())));
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) {
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() {
    map.clear();
  }

  @Override
  public Iterable<LongWritable> getPartitionDestinationVertices(
      int partitionId) {
    Long2DoubleOpenHashMap partitionMap = map.get(partitionId);
    List<LongWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    LongIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new LongWritable(iterator.nextLong()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    Long2DoubleOpenHashMap partitionMap = map.get(partitionId);
    out.writeInt(partitionMap.size());
    ObjectIterator<Long2DoubleMap.Entry> iterator =
        partitionMap.long2DoubleEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2DoubleMap.Entry entry = iterator.next();
      out.writeLong(entry.getLongKey());
      out.writeDouble(entry.getDoubleValue());
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Long2DoubleOpenHashMap partitionMap = new Long2DoubleOpenHashMap(size);
    while (size-- > 0) {
      long vertexId = in.readLong();
      double message = in.readDouble();
      partitionMap.put(vertexId, message);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
