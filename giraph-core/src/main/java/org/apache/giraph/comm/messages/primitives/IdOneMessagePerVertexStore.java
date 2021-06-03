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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Special message store to be used when IDs are primitive and message doesn't
 * need to be, and message combiner is used.
 * Data is backed by primitive keyed maps in order to decrease number of
 * objects and get better performance.
 * (keys are using primitives, values are using objects, even if they
 * are primitive)
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
public class IdOneMessagePerVertexStore<I extends WritableComparable,
    M extends Writable> implements MessageStore<I, M> {
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Basic2ObjectMap<I, M>> map;
  /** Message value factory */
  private final MessageValueFactory<M> messageValueFactory;
  /** Message messageCombiner */
  private final MessageCombiner<? super I, M> messageCombiner;
  /** Partition split info */
  private final PartitionSplitInfo<I> partitionInfo;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;
  /** Vertex id TypeOps */
  private final PrimitiveIdTypeOps<I> idTypeOps;
  /** WritableWriter for values in this message store */
  private final WritableWriter<M> messageWriter = new WritableWriter<M>() {
    @Override
    public M readFields(DataInput in) throws IOException {
      M message = messageValueFactory.newInstance();
      message.readFields(in);
      return message;
    }

    @Override
    public void write(DataOutput out, M value) throws IOException {
      value.write(out);
    }
  };

  /**
   * Constructor
   *
   * @param messageValueFactory Message value factory
   * @param partitionInfo Partition split info
   * @param messageCombiner Message messageCombiner
   * @param config Config
   */
  public IdOneMessagePerVertexStore(
      MessageValueFactory<M> messageValueFactory,
      PartitionSplitInfo<I> partitionInfo,
      MessageCombiner<? super I, M> messageCombiner,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    this.partitionInfo = partitionInfo;
    this.config = config;
    this.messageValueFactory = messageValueFactory;
    this.messageCombiner = messageCombiner;

    idTypeOps = TypeOpsUtils.getPrimitiveIdTypeOps(config.getVertexIdClass());

    map = new Int2ObjectOpenHashMap<>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      Basic2ObjectMap<I, M> partitionMap = idTypeOps.create2ObjectOpenHashMap(
        Math.max(10, (int) partitionInfo.getPartitionVertexCount(partitionId)),
        messageWriter
      );
      map.put(partitionId, partitionMap);
    }
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  private Basic2ObjectMap<I, M> getPartitionMap(I vertexId) {
    return map.get(partitionInfo.getPartitionId(vertexId));
  }

  @Override
  public void addPartitionMessages(
      int partitionId,
      VertexIdMessages<I, M> messages) {
    Basic2ObjectMap<I, M> partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageIterator<I, M>
          iterator = messages.getVertexIdMessageIterator();
      // This loop is a little complicated as it is optimized to only create
      // the minimal amount of vertex id and message objects as possible.
      while (iterator.hasNext()) {
        iterator.next();
        I vertexId = iterator.getCurrentVertexId();
        M currentMessage =
            partitionMap.get(iterator.getCurrentVertexId());
        if (currentMessage == null) {
          M newMessage = messageCombiner.createInitialMessage();
          currentMessage = partitionMap.put(
              iterator.getCurrentVertexId(), newMessage);
          if (currentMessage == null) {
            currentMessage = newMessage;
          }
        }
        messageCombiner.combine(vertexId, currentMessage,
          iterator.getCurrentMessage());
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
    Basic2ObjectMap<I, M> partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      M currentMessage = partitionMap.get(vertexId);
      if (currentMessage == null) {
        M newMessage = messageCombiner.createInitialMessage();
        currentMessage = partitionMap.put(vertexId, newMessage);
        if (currentMessage == null) {
          currentMessage = newMessage;
        }
      }
      messageCombiner.combine(vertexId, currentMessage, message);
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
    Basic2ObjectMap<I, M> partitionMessages = map.get(partitionId);
    return partitionMessages != null && partitionMessages.size() != 0;
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    Basic2ObjectMap<I, M> partitionMap = getPartitionMap(vertexId);
    if (!partitionMap.containsKey(vertexId)) {
      return EmptyIterable.get();
    } else {
      return Collections.singleton(partitionMap.get(vertexId));
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
  public Iterable<I> getPartitionDestinationVertices(
      int partitionId) {
    Basic2ObjectMap<I, M> partitionMap = map.get(partitionId);
    List<I> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    Iterator<I> iterator = partitionMap.fastKeyIterator();
    while (iterator.hasNext()) {
      vertices.add(idTypeOps.createCopy(iterator.next()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    Basic2ObjectMap<I, M> partitionMap = map.get(partitionId);
    partitionMap.write(out);
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    Basic2ObjectMap<I, M> partitionMap = idTypeOps.create2ObjectOpenHashMap(
        messageWriter);
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
