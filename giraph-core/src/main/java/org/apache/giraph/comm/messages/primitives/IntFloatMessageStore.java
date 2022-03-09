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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Special message store to be used when ids are IntWritable and messages
 * are FloatWritable and messageCombiner is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 */
public class IntFloatMessageStore
    implements MessageStore<IntWritable, FloatWritable> {
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Int2FloatOpenHashMap> map;
  /** Message messageCombiner */
  private final
  MessageCombiner<? super IntWritable, FloatWritable> messageCombiner;
  /** Partition split info */
  private final PartitionSplitInfo<IntWritable> partitionInfo;

  /**
   * Constructor
   *
   * @param partitionInfo Partition split info
   * @param messageCombiner Message messageCombiner
   */
  public IntFloatMessageStore(
    PartitionSplitInfo<IntWritable> partitionInfo,
    MessageCombiner<? super IntWritable, FloatWritable> messageCombiner
  ) {
    this.partitionInfo = partitionInfo;
    this.messageCombiner = messageCombiner;

    map = new Int2ObjectOpenHashMap<Int2FloatOpenHashMap>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      Int2FloatOpenHashMap partitionMap = new Int2FloatOpenHashMap(
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
  private Int2FloatOpenHashMap getPartitionMap(IntWritable vertexId) {
    return map.get(partitionInfo.getPartitionId(vertexId));
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<IntWritable, FloatWritable> messages) {
    IntWritable reusableVertexId = new IntWritable();
    FloatWritable reusableMessage = new FloatWritable();
    FloatWritable reusableCurrentMessage = new FloatWritable();

    Int2FloatOpenHashMap partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageIterator<IntWritable, FloatWritable>
          iterator = messages.getVertexIdMessageIterator();
      while (iterator.hasNext()) {
        iterator.next();
        int vertexId = iterator.getCurrentVertexId().get();
        float message = iterator.getCurrentMessage().get();
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
    IntWritable vertexId,
    FloatWritable message
  ) throws IOException {
    Int2FloatOpenHashMap partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      float originalValue = partitionMap.get(vertexId.get());
      FloatWritable originalMessage = new FloatWritable(originalValue);
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
  public boolean hasMessagesForVertex(IntWritable vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId.get());
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Int2FloatOpenHashMap partitionMessages = map.get(partitionId);
    return partitionMessages != null && !partitionMessages.isEmpty();
  }

  @Override
  public Iterable<FloatWritable> getVertexMessages(
      IntWritable vertexId) {
    Int2FloatOpenHashMap partitionMap = getPartitionMap(vertexId);
    if (!partitionMap.containsKey(vertexId.get())) {
      return EmptyIterable.get();
    } else {
      return Collections.singleton(
          new FloatWritable(partitionMap.get(vertexId.get())));
    }
  }

  @Override
  public void clearVertexMessages(IntWritable vertexId) {
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() {
    map.clear();
  }

  @Override
  public Iterable<IntWritable> getPartitionDestinationVertices(
      int partitionId) {
    Int2FloatOpenHashMap partitionMap = map.get(partitionId);
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
    Int2FloatOpenHashMap partitionMap = map.get(partitionId);
    out.writeInt(partitionMap.size());
    ObjectIterator<Int2FloatMap.Entry> iterator =
        partitionMap.int2FloatEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Int2FloatMap.Entry entry = iterator.next();
      out.writeInt(entry.getIntKey());
      out.writeFloat(entry.getFloatValue());
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Int2FloatOpenHashMap partitionMap = new Int2FloatOpenHashMap(size);
    while (size-- > 0) {
      int vertexId = in.readInt();
      float message = in.readFloat();
      partitionMap.put(vertexId, message);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
