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

import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract class for {@link MessageStore} which allows any kind
 * of object to hold messages for one vertex.
 * Simple in memory message store implemented with a two level concurrent
 * hash map.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <T> Type of object which holds messages for one vertex
 */
public abstract class SimpleMessageStore<I extends WritableComparable,
    M extends Writable, T> implements MessageStore<I, M>  {
  /** Message class */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Partition split info */
  protected final PartitionSplitInfo<I> partitionInfo;
  /** Map from partition id to map from vertex id to messages for that vertex */
  protected final ConcurrentMap<Integer, ConcurrentMap<I, T>> map;
  /** Giraph configuration */
  protected final ImmutableClassesGiraphConfiguration<I, ?, ?> config;

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param partitionInfo Partition split info
   * @param config Giraph configuration
   */
  public SimpleMessageStore(
      MessageValueFactory<M> messageValueFactory,
      PartitionSplitInfo<I> partitionInfo,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    this.messageValueFactory = messageValueFactory;
    this.partitionInfo = partitionInfo;
    this.config = config;
    map = new MapMaker().concurrencyLevel(
        config.getNettyServerExecutionConcurrency()).makeMap();
  }

  /**
   * Get messages as an iterable from message storage
   *
   * @param messages Message storage
   * @return Messages as an iterable
   */
  protected abstract Iterable<M> getMessagesAsIterable(T messages);

  /**
   * Get number of messages in partition map
   *
   * @param partitionMap Partition map in which to count messages
   * @return Number of messages in partition map
   */
  protected abstract int getNumberOfMessagesIn(
      ConcurrentMap<I, T> partitionMap);

  /**
   * Write message storage to {@link DataOutput}
   *
   * @param messages Message storage
   * @param out Data output
   * @throws IOException
   */
  protected abstract void writeMessages(T messages, DataOutput out) throws
      IOException;

  /**
   * Read message storage from {@link DataInput}
   *
   * @param in Data input
   * @return Message storage
   * @throws IOException
   */
  protected abstract T readFieldsForMessages(DataInput in) throws IOException;

  /**
   * Get id of partition which holds vertex with selected id
   *
   * @param vertexId Id of vertex
   * @return Id of partiton
   */
  protected int getPartitionId(I vertexId) {
    return partitionInfo.getPartitionId(vertexId);
  }

  /**
   * If there is already a map of messages related to the partition id
   * return that map, otherwise create a new one, put it in global map and
   * return it.
   *
   * @param partitionId Id of partition
   * @return Message map for this partition
   */
  protected ConcurrentMap<I, T> getOrCreatePartitionMap(int partitionId) {
    ConcurrentMap<I, T> partitionMap = map.get(partitionId);
    if (partitionMap == null) {
      ConcurrentMap<I, T> tmpMap = new MapMaker().concurrencyLevel(
          config.getNettyServerExecutionConcurrency()).makeMap();
      partitionMap = map.putIfAbsent(partitionId, tmpMap);
      if (partitionMap == null) {
        partitionMap = tmpMap;
      }
    }
    return partitionMap;
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    ConcurrentMap<I, ?> partitionMap = map.get(partitionId);
    return (partitionMap == null) ? Collections.<I>emptyList() :
        partitionMap.keySet();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    return partitionMap != null && partitionMap.containsKey(vertexId);
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    ConcurrentMap<I, T> partitionMap = map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return Collections.<M>emptyList();
    }
    T messages = partitionMap.get(vertexId);
    return (messages == null) ? Collections.<M>emptyList() :
        getMessagesAsIterable(messages);
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    ConcurrentMap<I, T> partitionMap = map.get(partitionId);
    out.writeBoolean(partitionMap != null);
    if (partitionMap != null) {
      out.writeInt(partitionMap.size());
      for (Map.Entry<I, T> entry : partitionMap.entrySet()) {
        entry.getKey().write(out);
        writeMessages(entry.getValue(), out);
      }
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      ConcurrentMap<I, T> partitionMap = Maps.newConcurrentMap();
      int numVertices = in.readInt();
      for (int v = 0; v < numVertices; v++) {
        I vertexId = config.createVertexId();
        vertexId.readFields(in);
        partitionMap.put(vertexId, readFieldsForMessages(in));
      }
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void clearVertexMessages(I vertexId) {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap != null) {
      partitionMap.remove(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) {
    map.remove(partitionId);
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    ConcurrentMap<I, T> partitionMessages = map.get(partitionId);
    return partitionMessages != null && !partitionMessages.isEmpty();
  }

  @Override
  public void clearAll() {
    map.clear();
  }
}
