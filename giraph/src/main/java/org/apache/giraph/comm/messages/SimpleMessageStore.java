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
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.VertexIdMessageCollection;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

/**
 * Simple in memory message store implemented with a two level concurrent
 * hash map.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SimpleMessageStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements MessageStoreByPartition<I, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(SimpleMessageStore.class);
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /**
   * Internal message map, from partition id to map from vertex id to
   * messages
   */
  private final ConcurrentMap<Integer, ConcurrentMap<I, Collection<M>>> map;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> config;
  /** Combiner for messages */
  private final VertexCombiner<I, M> combiner;

  /**
   * @param service  Service worker
   * @param combiner Combiner for messages
   * @param config   Hadoop configuration
   */
  SimpleMessageStore(CentralizedServiceWorker<I, V, E, M> service,
      VertexCombiner<I, M> combiner,
      ImmutableClassesGiraphConfiguration<I, V, E, M> config) {
    this.service = service;
    map = new MapMaker().concurrencyLevel(
        config.getNettyServerExecutionConcurrency()).makeMap();
    this.combiner = combiner;
    this.config = config;
  }

  @Override
  public void addVertexMessages(I vertexId,
      Collection<M> messages) throws IOException {
    int partitionId = getPartitionId(vertexId);
    ConcurrentMap<I, Collection<M>> partitionMap =
        getOrCreatePartitionMap(partitionId);
    Collection<M> currentMessages =
      CollectionUtils.addConcurrent(vertexId, messages, partitionMap);
    if (combiner != null) {
      synchronized (currentMessages) {
        currentMessages =
          Lists.newArrayList(combiner.combine(vertexId, currentMessages));
        partitionMap.put(vertexId, currentMessages);
      }
    }
  }

  @Override
  public void addMessages(Map<I, Collection<M>> messages) throws IOException {
    for (Entry<I, Collection<M>> entry : messages.entrySet()) {
      addVertexMessages(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void addPartitionMessages(Map<I, Collection<M>> messages,
      int partitionId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        getOrCreatePartitionMap(partitionId);

    for (Entry<I, Collection<M>> entry : messages.entrySet()) {
      Collection<M> currentMessages =
          CollectionUtils.addConcurrent(
              entry.getKey(), entry.getValue(), partitionMap);
      if (combiner != null) {
        synchronized (currentMessages) {
          currentMessages =
              Lists.newArrayList(combiner.combine(entry.getKey(),
                  currentMessages));
          partitionMap.put(entry.getKey(), currentMessages);
        }
      }
    }
  }

  @Override
  public void addPartitionMessages(VertexIdMessageCollection<I, M> messages,
      int partitionId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        getOrCreatePartitionMap(partitionId);

    VertexIdMessageCollection<I, M>.Iterator iterator = messages.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      I vertexId = iterator.getCurrentFirst();
      M message = iterator.getCurrentSecond();
      Collection<M> currentMessages = partitionMap.get(vertexId);
      if (currentMessages == null) {
        Collection<M> newMessages = Lists.newArrayList(message);
        currentMessages = partitionMap.putIfAbsent(vertexId, newMessages);
      }
      // if vertex messages existed before, or putIfAbsent didn't put new list
      if (currentMessages != null) {
        synchronized (currentMessages) {
          currentMessages.add(message);
          if (combiner != null) {
            currentMessages =
                Lists.newArrayList(combiner.combine(vertexId,
                    currentMessages));
            partitionMap.put(vertexId, currentMessages);
          }
        }
      }
    }
  }

  /**
   * If there is already a map of messages related to the partition id
   * return that map, otherwise create a new one, put it in global map and
   * return it.
   *
   * @param partitionId Id of partition
   * @return Message map for this partition
   */
  private ConcurrentMap<I, Collection<M>> getOrCreatePartitionMap(
      int partitionId) {
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    if (partitionMap == null) {
      ConcurrentMap<I, Collection<M>> tmpMap =
          new MapMaker().concurrencyLevel(
              config.getNettyServerExecutionConcurrency()).
              makeMap();
      partitionMap = map.putIfAbsent(partitionId, tmpMap);
      if (partitionMap == null) {
        partitionMap = map.get(partitionId);
      }
    }
    return partitionMap;
  }

  @Override
  public Collection<M> getVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return Collections.<M>emptyList();
    }
    Collection<M> messages = partitionMap.get(vertexId);
    return (messages == null) ? Collections.<M>emptyList() : messages;
  }

  @Override
  public int getNumberOfMessages() {
    int numberOfMessages = 0;
    for (ConcurrentMap<I, Collection<M>> partitionMap : map.values()) {
      for (Collection<M> messages : partitionMap.values()) {
        numberOfMessages += messages.size();
      }
    }
    return numberOfMessages;
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitionId(vertexId));
    return (partitionMap == null) ? false : partitionMap.containsKey(vertexId);
  }

  @Override
  public Iterable<I> getDestinationVertices() {
    List<I> vertices = Lists.newArrayList();
    for (ConcurrentMap<I, Collection<M>> partitionMap : map.values()) {
      vertices.addAll(partitionMap.keySet());
    }
    return vertices;
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    return (partitionMap == null) ? Collections.<I>emptyList() :
        partitionMap.keySet();
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap != null) {
      partitionMap.remove(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.remove(partitionId);
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  /**
   * Get id of partition which holds vertex with selected id
   *
   * @param vertexId Id of vertex
   * @return Id of partiton
   */
  private int getPartitionId(I vertexId) {
    return service.getVertexPartitionOwner(vertexId).getPartitionId();
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    out.writeBoolean(partitionMap != null);
    if (partitionMap != null) {
      out.writeInt(partitionMap.size());
      for (Entry<I, Collection<M>> entry : partitionMap.entrySet()) {
        entry.getKey().write(out);
        out.writeInt(entry.getValue().size());
        for (M message : entry.getValue()) {
          message.write(out);
        }
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(map.size());
    for (int partitionId : map.keySet()) {
      out.writeInt(partitionId);
      writePartition(out, partitionId);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      ConcurrentMap<I, Collection<M>> partitionMap = Maps.newConcurrentMap();
      int numVertices = in.readInt();
      for (int v = 0; v < numVertices; v++) {
        I vertexId = config.createVertexId();
        vertexId.readFields(in);
        int numMessages = in.readInt();
        List<M> messages = Lists.newArrayList();
        for (int m = 0; m < numMessages; m++) {
          M message = config.createMessageValue();
          message.readFields(in);
          messages.add(message);
        }
        partitionMap.put(vertexId, messages);
      }
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numPartitions = in.readInt();
    for (int p = 0; p < numPartitions; p++) {
      int partitionId = in.readInt();
      readFieldsForPartition(in, partitionId);
    }
  }


  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <V>     Vertex data
   * @param <E>     Edge data
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, M extends Writable>
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
      CentralizedServiceWorker<I, V, E, M> service,
      ImmutableClassesGiraphConfiguration<I, V, E, M> config) {
    return new Factory<I, V, E, M>(service, config);
  }

  /**
   * Factory for {@link SimpleMessageStore}
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      V extends Writable, E extends Writable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, V, E, M> service;
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration<I, V, E, M> config;
    /** Combiner for messages */
    private final VertexCombiner<I, M> combiner;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, V, E, M> service,
        ImmutableClassesGiraphConfiguration<I, V, E, M> config) {
      this.service = service;
      this.config = config;
      if (config.getVertexCombinerClass() == null) {
        combiner = null;
      } else {
        combiner = config.createVertexCombiner();
      }
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new SimpleMessageStore(service, combiner, config);
    }
  }
}
