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

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.util.List;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> message type
 * @param <T> datastructure used to hold messages
 */
public abstract class LongAbstractStore<M extends Writable, T>
  implements MessageStore<LongWritable, M> {
  /** Message value factory */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Map from partition id to map from vertex id to message */
  protected final
  Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<T>> map;
  /** Service worker */
  protected final PartitionSplitInfo<LongWritable> partitionInfo;
  /** Giraph configuration */
  protected final ImmutableClassesGiraphConfiguration<LongWritable, ?, ?>
  config;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param partitionInfo       Partition split info
   * @param config              Hadoop configuration
   */
  public LongAbstractStore(
      MessageValueFactory<M> messageValueFactory,
      PartitionSplitInfo<LongWritable> partitionInfo,
      ImmutableClassesGiraphConfiguration<LongWritable, Writable, Writable>
          config) {
    this.messageValueFactory = messageValueFactory;
    this.partitionInfo = partitionInfo;
    this.config = config;

    map = new Int2ObjectOpenHashMap<>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      Long2ObjectOpenHashMap<T> partitionMap = new Long2ObjectOpenHashMap<T>(
          (int) partitionInfo.getPartitionVertexCount(partitionId));
      map.put(partitionId, partitionMap);
    }
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  protected Long2ObjectOpenHashMap<T> getPartitionMap(
      LongWritable vertexId) {
    return map.get(partitionInfo.getPartitionId(vertexId));
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
    Long2ObjectOpenHashMap<T> partitionMessages = map.get(partitionId);
    return partitionMessages != null && !partitionMessages.isEmpty();
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
    Long2ObjectOpenHashMap<T> partitionMap =
        map.get(partitionId);
    List<LongWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    LongIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new LongWritable(iterator.nextLong()));
    }
    return vertices;
  }
}
